namespace Franz

/// The valid requiredAcks values
type RequiredAcks =
    /// The server will not send any response (this is the only case where the server will not reply to a request). Currently not supported by the client.
    | NoResponse = 0s
    /// The server will wait the data is written to the local log before sending a response.
    | LocalLog = 1s
    // The server will block until the message is committed by all in sync replicas before sending a response
    | AllReplicas = -1s

[<AutoOpen>]
module Messages =
    open System.IO
    open Franz.Stream
    open Franz.Internal

    /// Message size
    type MessageSize = int32
    /// Valid API keys for requests
    type ApiKey =
        | ProduceRequest = 0
        | FetchRequest = 1
        | OffsetRequest = 2
        | MetadataRequest = 3
        | OffsetCommitRequest = 8
        | OffsetFetchRequest = 9
        | ConsumerMetadataRequest = 10
    /// API versions, currently valid values are 0 and 1
    type ApiVersion = int16
    /// This is a user-supplied integer. It will be passed back in the response by the server, unmodified. It is useful for matching request and response between the client and server.
    type CorrelationId = int32
    /// This is a user supplied identifier for the client application. The user can use any identifier they like and it will be used when logging errors, monitoring aggregates, etc.
    /// For example, one might want to monitor not just the requests per second overall, but the number coming from each client application (each of which could reside on multiple servers).
    /// This id acts as a logical grouping across all requests from a particular client.
    type ClientId = string
    /// This is the offset used in kafka as the log sequence number.
    type Offset = int64
    /// The CRC is the CRC32 of the remainder of the message bytes.
    type Crc = int32
    /// This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 0.
    type MagicByte = int8
    /// This byte holds metadata attributes about the message. The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
    type Attributes = int8
    /// Possible error codes from brokers
    type ErrorCode =
        | NoError = 0
        | Unknown = -1
        | OffsetOutOfRange = 1
        | InvalidMessage = 2
        | UnknownTopicOrPartition = 3
        | InvalidMessageSize = 4
        | LeaderNotAvailable = 5
        | NotLeaderForPartition = 6
        | RequestTimedOut = 7
        | ReplicaNotAvailable = 9
        | MessageSizeTooLarge = 10
        | OffsetMetadataTooLarge = 12
        | OffsetLoadInProgress = 14
        | ConsumerCoordinatorNotAvailable = 15
        | NotCoordinatorForConsumer = 16
    /// Type for broker and partition ids
    type Id = int32
    /// The set of alive nodes that currently acts as slaves for the leader for this partition.
    type Replicas = Id array
    /// The set subset of the replicas that are "caught up" to the leader. 
    type Isr = Id array

    /// Request base class
    [<AbstractClass>]
    type Request<'TResponse>() =
        /// The API key.
        abstract member ApiKey : ApiKey with get
        /// The API version.
        abstract member ApiVersion : ApiVersion with get
        /// The client id.
        abstract member ClientId : ClientId with get
        /// Serializes the request.
        abstract member SerializeMessage : Stream -> unit
        /// Deserialize the response.
        abstract member DeserializeResponse : Stream -> 'TResponse

        default self.ApiVersion = int16 0
        default self.ClientId = "Franz"
        
        /// The correlation id.
        member val CorrelationId : CorrelationId = 0 with get, set

        /// Serialize the request header.
        member self.SerializeHeader (stream : Stream) =
            stream |> BigEndianWriter.WriteInt32 4 // Allocate space for size
            stream |> BigEndianWriter.WriteInt16 (self.ApiKey |> int |> int16)
            stream |> BigEndianWriter.WriteInt16 self.ApiVersion
            stream |> BigEndianWriter.WriteInt32 self.CorrelationId
            stream |> BigEndianWriter.WriteString self.ClientId
    
        /// Write the message size.
        member self.WriteSize (stream : Stream) =
            let size = int32 stream.Length
            stream.Seek(int64 0, SeekOrigin.Begin) |> ignore
            stream |> BigEndianWriter.WriteInt32 (size - 4)
            stream.Seek(int64 0, SeekOrigin.Begin) |> ignore
            let buffer = Array.zeroCreate(size)
            stream.Read(buffer, 0, size) |> ignore
            buffer

        /// Serialize the request.
        member self.Serialize(stream) =
            let memoryStream = new MemoryStream()
            self.SerializeHeader(memoryStream)
            self.SerializeMessage(memoryStream)
            let buffer = self.WriteSize(memoryStream)
            buffer |> BigEndianWriter.Write stream

    /// Message in a messageset.
    [<NoEquality;NoComparison>]
    type Message =
        {
            Crc : Crc;
            MagicByte : MagicByte;
            Attributes : Attributes;
            Key : byte array;
            Value : byte array;
        }

    /// Type for messageset.
    type MessageSet(offset : Offset, size : MessageSize, message : Message) =
        /// Messageset header size
        static member private messageSetHeaderSize = 4 + 8
        /// Offset of the message. When sending a message, this can be any value.
        member val Offset = offset with get
        /// The size of the message in the messageset.
        member val MessageSize = size with get
        /// The total size of the messageset.
        member val MessageSetSize = size + MessageSet.messageSetHeaderSize
        /// The message.
        member val Message = message with get
        
        /// Create a new messageset.
        static member Create(offset : Offset, attributes : Attributes, key, value) =
            let stream = new MemoryStream()
            stream |> BigEndianWriter.WriteInt8 (int8 0)
            stream |> BigEndianWriter.WriteInt8 attributes
            stream |> BigEndianWriter.WriteBytes key
            stream |> BigEndianWriter.WriteBytes value
            let content = Array.zeroCreate(int stream.Length)
            stream.Seek(int64 0, SeekOrigin.Begin) |> ignore
            stream.Read(content, 0, int stream.Length) |> ignore
            let crc = crc32 content
            let message = { Crc = crc; MagicByte = int8 0; Attributes = attributes; Key = key; Value = value; }
            new MessageSet(offset, content.Length + 4, message)

        /// Serialize the messageset.
        member self.Serialize (stream : Stream) =
            stream |> BigEndianWriter.WriteInt64 self.Offset
            stream |> BigEndianWriter.WriteInt32 self.MessageSize
            stream |> BigEndianWriter.WriteInt32 self.Message.Crc
            stream |> BigEndianWriter.WriteInt8 self.Message.MagicByte
            stream |> BigEndianWriter.WriteInt8 self.Message.Attributes
            stream |> BigEndianWriter.WriteBytes self.Message.Key
            stream |> BigEndianWriter.WriteBytes self.Message.Value

        /// Deserialize the messageset.
        static member Deserialize (buffer : byte array) =
            let stream = new MemoryStream(buffer)
            let rec decodeMessageSet list =
                let bytesAvailable = buffer.Length - int stream.Position
                if bytesAvailable > MessageSet.messageSetHeaderSize then
                    let offset = stream |> BigEndianReader.ReadInt64
                    let messageSize = stream |> BigEndianReader.ReadInt32
                    if bytesAvailable - MessageSet.messageSetHeaderSize >= messageSize then
                        let message =
                            {
                                Crc = stream |> BigEndianReader.ReadInt32;
                                MagicByte = stream |> BigEndianReader.ReadInt8;
                                Attributes = stream |> BigEndianReader.ReadInt8;
                                Key = stream |> BigEndianReader.ReadBytes;
                                Value = stream |> BigEndianReader.ReadBytes
                            }
                        let messageSet = new MessageSet(offset, messageSize, message)
                        decodeMessageSet (messageSet :: list)
                    else
                        dprintfn "Received partial message, skipping..."
                        stream |> BigEndianReader.Read (bytesAvailable - 12) |> ignore
                        decodeMessageSet list
                else
                    list
            decodeMessageSet [] |> Seq.toArray

    /// Broker
    [<NoEquality;NoComparison>] type Broker = { NodeId : int32; Host : string; Port : int32; }
    /// PartitionMetadata
    [<NoEquality;NoComparison>] type PartitionMetadata = { ErrorCode : ErrorCode; PartitionId : int32; Leader : Id; Replicas : Replicas; Isr : Isr; }
    /// TopicMetadata
    [<NoEquality;NoComparison>] type TopicMetadata = { ErrorCode : ErrorCode; Name : string; PartitionMetadata : PartitionMetadata array; }
    /// PartitionProduceRequest
    [<NoEquality;NoComparison>] type PartitionProduceRequest = { Id : int32; MessageSetSize : MessageSize; MessageSet : MessageSet; }
    /// PartitionProduceResponse
    [<NoEquality;NoComparison>] type PartitionProduceResponse = { Id : int32; ErrorCode : ErrorCode; Offset : Offset }
    /// TopicProduceRequest
    [<NoEquality;NoComparison>] type TopicProduceRequest = { Name : string; Partitions : PartitionProduceRequest array }
    /// TopicProduceResponse
    [<NoEquality;NoComparison>] type TopicProduceResponse = { Name : string; Partitions : PartitionProduceResponse array }
    /// FetchPartitionResponse
    [<NoEquality;NoComparison>] type FetchPartitionResponse = { Id : Id; ErrorCode : ErrorCode; HighwaterMarkOffset : Offset; MessageSetSize : MessageSize; MessageSets : MessageSet array; }
    /// FetchTopicResponse
    [<NoEquality;NoComparison>] type FetchTopicResponse = { TopicName : string; Partitions : FetchPartitionResponse array; }
    /// FetchPartitionRequest
    [<NoEquality;NoComparison>] type FetchPartitionRequest = { Id : Id; FetchOffset : Offset; MaxBytes : int32; }
    /// FetchTopicRequest
    [<NoEquality;NoComparison>] type FetchTopicRequest = { Name : string; Partitions : FetchPartitionRequest array; }
    /// OffsetRequestPartition
    [<NoEquality;NoComparison>] type OffsetRequestPartition = { Id : Id; Time : int64; MaxNumberOfOffsets : int32; }
    /// OffsetRequestTopic
    [<NoEquality;NoComparison>] type OffsetRequestTopic = { Name : string; Partitions : OffsetRequestPartition array }
    /// PartitionOffset
    [<NoEquality;NoComparison>] type PartitionOffset = { Id : Id; ErrorCode : ErrorCode; Offsets : Offset array; }
    /// OffsetResponseTopic
    [<NoEquality;NoComparison>] type OffsetResponseTopic = { Name : string; Partitions : PartitionOffset array; }
    /// OffsetCommitRequestV1Partition
    [<NoEquality;NoComparison>] type OffsetCommitRequestV1Partition = { Id : Id; Offset : Offset; TimeStamp : int64; Metadata : string; }
    /// OffsetCommitRequestV1Topic
    [<NoEquality;NoComparison>] type OffsetCommitRequestV1Topic = { Name : string; Partitions : OffsetCommitRequestV1Partition array; }
    /// OffsetCommitResponsePartition
    [<NoEquality;NoComparison>] type OffsetCommitResponsePartition = { Id : Id; ErrorCode : ErrorCode;  }
    /// OffsetCommitResponseTopic
    [<NoEquality;NoComparison>] type OffsetCommitResponseTopic = { Name : string; Partitions : OffsetCommitResponsePartition array; }
    /// OffsetFetchRequestTopic
    [<NoEquality;NoComparison>] type OffsetFetchRequestTopic = { Name : string; Partitions : Id array }
    /// OffsetFetchResponsePartition
    [<NoEquality;NoComparison>] type OffsetFetchResponsePartition = { Id : Id; Offset : Offset; Metadata : string; ErrorCode : ErrorCode; }
    /// OffsetFetchResponseTopic
    [<NoEquality;NoComparison>] type OffsetFetchResponseTopic = { Name : string; Partitions : OffsetFetchResponsePartition array; }
    /// OffsetCommitRequestV0Partition
    [<NoEquality;NoComparison>] type OffsetCommitRequestV0Partition = { Id : Id; Offset : Offset; Metadata : string; }
    /// OffsetCommitRequestV0Topic
    [<NoEquality;NoComparison>] type OffsetCommitRequestV0Topic = { Name : string; Partitions : OffsetCommitRequestV0Partition array; }

    /// Metadata response
    [<NoEquality;NoComparison>]
    type MetadataResponse =
        {CorrelationId : CorrelationId; Brokers : Broker array; TopicMetadata : TopicMetadata array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readBrokers list count =
                match count with
                | 0 -> list
                | _ ->
                    let broker = { NodeId = memoryStream |> BigEndianReader.ReadInt32; Host = memoryStream |> BigEndianReader.ReadString; Port = memoryStream |> BigEndianReader.ReadInt32 }
                    readBrokers (broker :: list) (count - 1)
            let numberOfBrokers = memoryStream |> BigEndianReader.ReadInt32
            let brokers = readBrokers [] numberOfBrokers
            let rec readIds list count =
                match count with
                | 0 -> list
                | _ ->
                    let id = memoryStream |> BigEndianReader.ReadInt32
                    readIds (id :: list) (count - 1)
            let rec readPartitionMetadata list count =
                match count with
                | 0 -> list
                | _ ->
                    let errorCode = memoryStream |> BigEndianReader.ReadInt16
                    let partitionId = memoryStream |> BigEndianReader.ReadInt32
                    let leader = memoryStream |> BigEndianReader.ReadInt32
                    let replicaCount = memoryStream |> BigEndianReader.ReadInt32
                    let replicas = readIds [] replicaCount
                    let isrs = readIds [] (memoryStream |> BigEndianReader.ReadInt32)
                    let metadata = { ErrorCode = enum<ErrorCode>(int32 errorCode); PartitionId = partitionId; Leader = leader; Replicas = replicas |> List.toArray; Isr = isrs |> List.toArray }
                    readPartitionMetadata (metadata :: list) (count - 1)
            let rec readTopicMetadata list count =
                match count with
                | 0 -> list
                | _ ->
                    let errorCode = memoryStream |> BigEndianReader.ReadInt16
                    let topicName = memoryStream |> BigEndianReader.ReadString
                    let numberOfPartitionMetadata = memoryStream |> BigEndianReader.ReadInt32
                    let partitionMetadata = readPartitionMetadata [] numberOfPartitionMetadata
                    let metadata = { ErrorCode = enum<ErrorCode>(int32 errorCode); Name = topicName; PartitionMetadata = partitionMetadata |> List.toArray; }
                    readTopicMetadata (metadata :: list) (count - 1)
            let numberOfMetadata = memoryStream |> BigEndianReader.ReadInt32
            let topicMetadata = readTopicMetadata [] numberOfMetadata
            { CorrelationId = correlationId; Brokers = brokers |> List.toArray; TopicMetadata = topicMetadata |> List.toArray }

    /// Produce response
    [<NoEquality;NoComparison>]
    type ProduceResponse =
        { CorrelationId : CorrelationId; Topics : TopicProduceResponse array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let buffer = stream |> BigEndianReader.Read messageSize
            let memoryStream = new MemoryStream(buffer)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readPartition list count =
                match count with
                | 0 -> list
                | _ ->
                    let partition = { PartitionProduceResponse.Id = memoryStream |> BigEndianReader.ReadInt32; ErrorCode = memoryStream |> BigEndianReader.ReadInt16 |> int |> enum<ErrorCode>; Offset = memoryStream |> BigEndianReader.ReadInt64; }
                    readPartition (partition :: list) (count - 1)
            let rec readTopic list count =
                match count with
                | 0 -> list
                | _ ->
                    let topic = { TopicProduceResponse.Name = memoryStream |> BigEndianReader.ReadString; Partitions = readPartition [] (memoryStream |> BigEndianReader.ReadInt32) |> List.toArray; }
                    readTopic (topic :: list) (count - 1)
            let numberOfTopics = (memoryStream |> BigEndianReader.ReadInt32)
            { ProduceResponse.CorrelationId = correlationId; Topics = readTopic [] numberOfTopics |> List.toArray }

    /// Fetch response
    [<NoEquality;NoComparison>]
    type FetchResponse =
        { CorrelationId : CorrelationId; Topics : FetchTopicResponse array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readPartition list count =
                match count with
                | 0 -> list
                | _ ->
                    let id = memoryStream |> BigEndianReader.ReadInt32
                    let errorCode = memoryStream |> BigEndianReader.ReadInt16 |> int |> enum<ErrorCode>
                    let highwaterMarkOffset = memoryStream |> BigEndianReader.ReadInt64
                    let messageSetSize = memoryStream |> BigEndianReader.ReadInt32
                    let messageSets = memoryStream |> BigEndianReader.Read messageSetSize |> MessageSet.Deserialize |> Array.rev
                    let partition = { FetchPartitionResponse.Id = id; ErrorCode = errorCode; HighwaterMarkOffset = highwaterMarkOffset; MessageSetSize = messageSetSize; MessageSets = messageSets }
                    readPartition (partition :: list) (count - 1)
            let rec readTopic list count =
                match count with
                | 0 -> list
                | _ ->
                    let topic = { FetchTopicResponse.TopicName = memoryStream |> BigEndianReader.ReadString; Partitions = readPartition [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }
                    readTopic (topic :: list) (count - 1)
            { FetchResponse.CorrelationId = correlationId; Topics = readTopic [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }

    /// An offset response.
    /// Contains the starting offset of each segment for the requested partition as well as the "log end offset" i.e. the offset of the next message that would be appended to the given partition.
    [<NoEquality;NoComparison>]
    type OffsetResponse =
        { CorrelationId : CorrelationId; Topics : OffsetResponseTopic array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readOffsets list count =
                match count with
                | 0 -> list
                | _ ->
                    let offset = memoryStream |> BigEndianReader.ReadInt64
                    readOffsets (offset :: list) (count - 1)
            let rec readPartition list count =
                match count with
                | 0 -> list
                | _ ->
                    let partition = { Id = memoryStream |> BigEndianReader.ReadInt32; ErrorCode = memoryStream |> BigEndianReader.ReadInt16 |> int32 |> enum<ErrorCode>; Offsets = readOffsets [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }
                    readPartition (partition :: list) (count - 1)
            let rec readTopic list count =
                match count with
                | 0 -> list
                | _ ->
                    let topic = { OffsetResponseTopic.Name = memoryStream |> BigEndianReader.ReadString; Partitions = readPartition [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }
                    readTopic (topic :: list) (count - 1)
            { CorrelationId = correlationId; OffsetResponse.Topics = readTopic [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }

    /// Consumer metadata response
    [<NoEquality;NoComparison>]
    type ConsumerMetadataResponse =
        { CorrelationId : CorrelationId; ErrorCode : ErrorCode; CoordinatorId : Id; CoordinatorHost : string; CoordinatorPort : int32; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            {
                CorrelationId = correlationId;
                ErrorCode = memoryStream |> BigEndianReader.ReadInt16 |> int |> enum<ErrorCode>;
                CoordinatorId = memoryStream |> BigEndianReader.ReadInt32;
                CoordinatorHost = memoryStream |> BigEndianReader.ReadString;
                CoordinatorPort = memoryStream |> BigEndianReader.ReadInt32
            }

    /// Offset commit response
    [<NoEquality;NoComparison>]
    type OffsetCommitResponse =
        { CorrelationId : CorrelationId; Topics : OffsetCommitResponseTopic array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readPartition list count =
                match count with
                | 0 -> list
                | _ ->
                    let partition = { OffsetCommitResponsePartition.Id = memoryStream |> BigEndianReader.ReadInt32; ErrorCode = memoryStream |> BigEndianReader.ReadInt16 |> int |> enum<ErrorCode> }
                    readPartition (partition :: list) (count - 1)
            let rec readTopic list count =
                match count with
                | 0 -> list
                | _ ->
                    let topic = { OffsetCommitResponseTopic.Name = memoryStream |> BigEndianReader.ReadString; Partitions = readPartition [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }
                    readTopic (topic :: list) (count - 1)
            { OffsetCommitResponse.CorrelationId = correlationId; Topics = readTopic [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }

    /// Offset fetch response
    [<NoEquality;NoComparison>]
    type OffsetFetchResponse =
        { CorrelationId : CorrelationId; Topics : OffsetFetchResponseTopic array; }
        /// Deserialize response from a stream
        static member Deserialize(stream) =
            let messageSize = stream |> BigEndianReader.ReadInt32
            dprintfn "Received message of size %i" messageSize
            let memoryStream = new MemoryStream(stream |> BigEndianReader.Read messageSize)
            let correlationId = memoryStream |> BigEndianReader.ReadInt32
            let rec readPartition list count =
                match count with
                | 0 -> list
                | _ ->
                    let partition = { Id = memoryStream |> BigEndianReader.ReadInt32; Offset = memoryStream |> BigEndianReader.ReadInt64; Metadata = memoryStream |> BigEndianReader.ReadString; ErrorCode = memoryStream |> BigEndianReader.ReadInt16 |> int |> enum<ErrorCode> }
                    readPartition (partition :: list) (count - 1)
            let rec readTopic list count =
                match count with
                | 0 -> list
                | _ ->
                    let topic = { OffsetFetchResponseTopic.Name = memoryStream |> BigEndianReader.ReadString; Partitions = readPartition [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }
                    readTopic (topic :: list) (count - 1)
            { OffsetFetchResponse.CorrelationId = correlationId; Topics = readTopic [] (memoryStream |> BigEndianReader.ReadInt32) |> Seq.toArray }

    /// Metadata request
    type MetadataRequest(topicNames) =
        inherit Request<MetadataResponse>()
        /// The api key
        override self.ApiKey = ApiKey.MetadataRequest
        /// Gets or sets the topic names
        member val TopicNames : string array = topicNames with get, set
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteInt32 self.TopicNames.Length
            for topicName in self.TopicNames do
                stream |> BigEndianWriter.WriteString topicName
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            MetadataResponse.Deserialize stream
    
    /// Creates a offset request.
    /// This API describes the valid offset range available for a set of topic-partitions
    type OffsetRequest(replicaId : Id, topics : OffsetRequestTopic array) =
        inherit Request<OffsetResponse>()
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id.
        member val ReplicaId = replicaId with get, set
        /// Gets the topics.
        member val Topics = topics with get, set
        /// The api key
        override self.ApiKey = ApiKey.OffsetRequest
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteInt32 self.ReplicaId
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                for partition in topic.Partitions do
                    stream |> BigEndianWriter.WriteInt32 partition.Id
                    stream |> BigEndianWriter.WriteInt64 partition.Time
                    stream |> BigEndianWriter.WriteInt32 partition.MaxNumberOfOffsets
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            OffsetResponse.Deserialize(stream)

    /// Produce request
    type ProduceRequest(requiredAcks : RequiredAcks, timeout : int32, topics : TopicProduceRequest array) =
        inherit Request<ProduceResponse>()
        /// This field indicates how many acknowledgements the servers should receive before responding to the request
        member val RequiredAcks = requiredAcks with get
        /// This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks.
        /// The timeout is not an exact limit on the request time for a few reasons:
        /// (1) it does not include network latency,
        /// (2) the timer begins at the beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included,
        /// (3) we will not terminate a local write so if the local write time exceeds this timeout it will not be respected.
        member val Timeout = timeout with get
        /// Gets the topics
        member val Topics = topics with get
        /// The api key
        override self.ApiKey = ApiKey.ProduceRequest
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteInt16 (requiredAcks |> int16)
            stream |> BigEndianWriter.WriteInt32 self.Timeout
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                for partition in topic.Partitions do
                    stream |> BigEndianWriter.WriteInt32 partition.Id
                    stream |> BigEndianWriter.WriteInt32 partition.MessageSetSize
                    stream |> partition.MessageSet.Serialize
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            ProduceResponse.Deserialize(stream)

    /// Fetch request
    type FetchRequest(replicaId : Id, maxWaitTime : int32, minBytes : int32, topics : FetchTopicRequest array) =
        inherit Request<FetchResponse>()
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id.
        member val ReplicaId = replicaId with get
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
        member val MaxWaitTime = maxWaitTime with get
        /// Minimum number of bytes of messages that must be available to give a response.
        member val MinBytes = minBytes with get
        /// Gets the topics
        member val Topics = topics with get
        /// The api key
        override self.ApiKey = ApiKey.FetchRequest
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteInt32 self.ReplicaId
            stream |> BigEndianWriter.WriteInt32 self.MaxWaitTime
            stream |> BigEndianWriter.WriteInt32 self.MinBytes
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                for partition in topic.Partitions do
                    stream |> BigEndianWriter.WriteInt32 partition.Id
                    stream |> BigEndianWriter.WriteInt64 partition.FetchOffset
                    stream |> BigEndianWriter.WriteInt32 partition.MaxBytes
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            FetchResponse.Deserialize(stream)

    /// Offset fetch request
    type OffsetFetchRequest(consumerGroup : string, topics : OffsetFetchRequestTopic array, apiVersion) =
        inherit Request<OffsetFetchResponse>()
        /// Gets the consumer group
        member val ConsumerGroup = consumerGroup with get
        /// Gets the topics
        member val Topics = topics with get
        /// The api key
        override self.ApiKey = ApiKey.OffsetFetchRequest
        /// The api version
        override self.ApiVersion = apiVersion
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteString self.ConsumerGroup
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                topic.Partitions |> Array.iter (fun x -> stream |> BigEndianWriter.WriteInt32 x)
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            OffsetFetchResponse.Deserialize(stream)

    /// Offset commit request version 1
    type OffsetCommitV1Request(consumerGroup : string, consumerGroupGeneration : Id, consumerId : string, topics : OffsetCommitRequestV1Topic array) =
        inherit Request<OffsetCommitResponse>()
        /// Gets the consumer group
        member val ConsumerGroup = consumerGroup with get
        /// Gets the consumer group generation
        member val ConsumerGroupGeneration = consumerGroupGeneration with get
        /// Gets the consumer id
        member val ConsumerId = consumerId with get
        /// Gets the topics
        member val Topics = topics with get
        /// The api key
        override self.ApiKey = ApiKey.OffsetCommitRequest
        /// The api version
        override self.ApiVersion = int16 1
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteString self.ConsumerGroup
            stream |> BigEndianWriter.WriteInt32 self.ConsumerGroupGeneration
            stream |> BigEndianWriter.WriteString self.ConsumerId
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                for partition in topic.Partitions do
                    stream |> BigEndianWriter.WriteInt32 partition.Id
                    stream |> BigEndianWriter.WriteInt64 partition.Offset
                    stream |> BigEndianWriter.WriteInt64 partition.TimeStamp
                    stream |> BigEndianWriter.WriteString partition.Metadata
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            OffsetCommitResponse.Deserialize(stream)
            
    /// Offset commit request version 0
    type OffsetCommitV0Request(consumerGroup : string, topics : OffsetCommitRequestV0Topic array) =
        inherit Request<OffsetCommitResponse>()
        /// Gets the consumer group
        member val ConsumerGroup = consumerGroup with get
        /// Gets the topics
        member val Topics = topics with get
        /// The api key
        override self.ApiKey = ApiKey.OffsetCommitRequest
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteString self.ConsumerGroup
            stream |> BigEndianWriter.WriteInt32 self.Topics.Length
            for topic in self.Topics do
                stream |> BigEndianWriter.WriteString topic.Name
                stream |> BigEndianWriter.WriteInt32 topic.Partitions.Length
                for partition in topic.Partitions do
                    stream |> BigEndianWriter.WriteInt32 partition.Id
                    stream |> BigEndianWriter.WriteInt64 partition.Offset
                    stream |> BigEndianWriter.WriteString partition.Metadata
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            OffsetCommitResponse.Deserialize(stream)

    /// Consumer metadata request
    type ConsumerMetadataRequest(consumerGroup : string) =
        inherit Request<ConsumerMetadataResponse>()
        /// Gets the consumer group
        member val ConsumerGroup = consumerGroup with get
        /// The api key
        override self.ApiKey = ApiKey.ConsumerMetadataRequest
        /// Serialize the message
        override self.SerializeMessage(stream) =
            stream |> BigEndianWriter.WriteString self.ConsumerGroup
        /// Deserialize the response
        override self.DeserializeResponse(stream) =
            ConsumerMetadataResponse.Deserialize(stream)
