namespace Franz.HighLevel

open System
open System.Collections.Generic
open System.Text
open Franz
open Franz.Internal
open System.Collections.Concurrent
open Franz.Compression

module Seq =
    /// Helper function to convert a sequence to a List<T>
    let toBclList (x : 'a seq) =
        new List<_>(x)

type PartiontionIds = List<Id>
type NextPartitionId = Id
type TopicPartitions = Dictionary<string, (PartiontionIds * NextPartitionId)>

type IProducer =
    abstract member SendMessage : string * string * RequiredAcks * int * Id array -> unit
    abstract member SendMessage : string * string * Id array -> unit
    abstract member SendMessage : string * string -> unit
    abstract member SendMessages : string * string array * RequiredAcks * int * Id array -> unit
    abstract member SendMessages : string * string array * Id array -> unit
    abstract member SendMessages : string * string array -> unit
    abstract member SendMessages : string * string array * RequiredAcks * int -> unit
    abstract member SendMessage : string * string * RequiredAcks * int -> unit

/// High level kafka producer
type Producer(brokerSeeds, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let topicPartitions = new TopicPartitions()
    let sortTopicPartitions() =
        topicPartitions |> Seq.iter (fun kvp ->
            let (ids, _) = kvp.Value
            ids.Sort())
    let updateTopicPartitions (brokers : Broker seq) =
        brokers
        |> Seq.map (fun x -> x.LeaderFor)
        |> Seq.concat
        |> Seq.map (fun x -> (x.TopicName, x.PartitionIds))
        |> Seq.iter (fun (topic, partitions) ->
            if not <| topicPartitions.ContainsKey(topic) then
                topicPartitions.Add(topic, (partitions |> Seq.toBclList, 0))
            else
                let (ids, _) = topicPartitions.[topic]
                let partitionsToAdd = partitions |> Seq.filter (fun x -> ids |> Seq.exists (fun i -> i = x) |> not)
                ids.AddRange(partitionsToAdd))
        sortTopicPartitions()
    do
        brokerRouter.Error.Add(fun x -> dprintfn "%A" x)
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
    new (brokerSeeds) = new Producer(brokerSeeds, 10000)
    new (brokerSeeds, tcpTimeout) = new Producer(brokerSeeds, new BrokerRouter(tcpTimeout))
    /// Sends a message to the specified topic
    member self.SendMessages(topicName, message) =
        self.SendMessages(topicName, message, RequiredAcks.LocalLog, 500, null)
    /// Sends a message to the specified topic
    member self.SendMessages(topicName, message, partitionWhiteList) =
        self.SendMessages(topicName, message, RequiredAcks.LocalLog, 500, partitionWhiteList)
    /// Sends a message to the specified topic
    member __.SendMessages(topicName, messages : string array, requiredAcks, brokerProcessingTimeout, partitionWhiteList : Id array) =
        if disposed then invalidOp "Producer has been disposed"
        let rec innerSend() =
            let messageSets = messages |> Array.map (fun x -> MessageSet.Create(int64 -1, int8 0, null, Encoding.UTF8.GetBytes(x)))
            let partitionId =
                let success, result = topicPartitions.TryGetValue(topicName)
                if (not success) then
                    brokerRouter.GetBroker(topicName, 0) |> ignore
                    0
                else
                    let (partitionIds, nextId) = result
                    let filteredPartitionIds =
                        match partitionWhiteList with
                        | null | [||] -> partitionIds |> Seq.toBclList
                        | _ -> Set.intersect (Set.ofArray (partitionIds.ToArray())) (Set.ofArray partitionWhiteList) |> Seq.toBclList
                    let nextId =
                        if nextId = (filteredPartitionIds |> Seq.max) then filteredPartitionIds |> Seq.min
                        else filteredPartitionIds |> Seq.find (fun x -> x > nextId)
                    topicPartitions.[topicName] <- (filteredPartitionIds, nextId)
                    nextId
            let partitions = { PartitionProduceRequest.Id = partitionId; MessageSets = messageSets; TotalMessageSetsSize = messageSets |> Seq.sumBy (fun x -> x.MessageSetSize) }
            let topic = { TopicProduceRequest.Name = topicName; Partitions = [| partitions |] }
            let request = new ProduceRequest(requiredAcks, brokerProcessingTimeout, [| topic |])
            let broker = brokerRouter.GetBroker(topicName, partitionId)
            let rec trySend (broker : Broker) attempt =
                try
                    broker.Send(request)
                with
                | e ->
                    dprintfn "Got exception while sending request %s" e.Message
                    if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
                    else
                        brokerRouter.RefreshMetadata()
                        let newBroker = brokerRouter.GetBroker(topicName, partitionId)
                        trySend newBroker (attempt + 1)
            let response = trySend broker 0
            let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
            match partitionResponse.ErrorCode with
            | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable -> ()
            | ErrorCode.NotLeaderForPartition ->
                brokerRouter.RefreshMetadata()
                innerSend()
            | _ -> invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)
        innerSend()
    /// Get all available brokers
    member __.GetAllBrokers() =
        brokerRouter.GetAllBrokers()
    /// Releases all connections and disposes the producer
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IProducer with
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout, partitionWhiteList) =
            self.SendMessages(topicName, [| message |], requiredAcks, brokerProcessingTimeout, partitionWhiteList)
        member self.SendMessage(topicName, message) =
            self.SendMessages(topicName, [| message |])
        member self.SendMessage(topicName, message, partitionWhiteList) =
            self.SendMessages(topicName, [| message |], partitionWhiteList)
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout, partitionWhiteList) =
            self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout, partitionWhiteList)
        member self.SendMessages(topicName, messages, partitionWhiteList) = 
            self.SendMessages(topicName, messages, partitionWhiteList)
        member self.SendMessages(topicName, messages) = 
            self.SendMessages(topicName, messages)
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout, [||])
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, [| message |], requiredAcks, brokerProcessingTimeout, [||])
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// Information about offsets
type PartitionOffset = { PartitionId : Id; Offset : Offset; Metadata : string }

/// Interface for offset managers
type IConsumerOffsetManager =
    inherit IDisposable
    /// Fetch offset for the specified topic and partitions
    abstract member Fetch : string -> PartitionOffset array
    /// Commit offset for the specified topic and partitions
    abstract member Commit : string * PartitionOffset seq -> unit

/// Offset manager for version 0. This commits and fetches offset to/from Zookeeper instances.
type ConsumerOffsetManagerV0(brokerSeeds, topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let partitions = new ConcurrentDictionary<_, _>()
    let updatePartitions (brokers : Broker seq) =
        brokers
            |> Seq.map (fun x -> x.LeaderFor)
            |> Seq.concat
            |> Seq.filter (fun x -> x.TopicName = topicName)
            |> Seq.map (fun x -> x.PartitionIds)
            |> Seq.concat
            |> Seq.iter (fun x -> if partitions.ContainsKey(x) then () else partitions.TryAdd(x, null) |> ignore)
        brokers
            |> Seq.map (fun x -> x.LeaderFor)
            |> Seq.concat
            |> Seq.filter (fun x -> x.TopicName = topicName)
            |> Seq.map (fun x -> x.PartitionIds)
            |> Seq.concat
            |> Seq.filter (partitions.ContainsKey >> not)
            |> Seq.iter (partitions.TryRemove >> ignore)
    let refreshMetadataOnException f =
        try
            f()
        with
        | _ ->
            brokerRouter.RefreshMetadata()
            f()
    do
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updatePartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updatePartitions)
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV0(brokerSeeds, topicName, new BrokerRouter(tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
            if disposed then invalidOp "Offset manager has been disposed"
            let innerFetch () =
                let broker = brokerRouter.GetAllBrokers() |> Seq.head
                let request = new OffsetFetchRequest(consumerGroup, [| { OffsetFetchRequestTopic.Name = topicName; Partitions = partitions.Keys |> Seq.toArray } |], int16 0)
                let response = broker.Send(request)
                response.Topics
                    |> Seq.filter (fun x -> x.Name = topicName)
                    |> Seq.map (fun x -> x.Partitions)
                    |> Seq.concat
                    |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
                    |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = x.Metadata; Offset = x.Offset })
                    |> Seq.toArray
            refreshMetadataOnException innerFetch
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            let innerCommit () =
                let broker = brokerRouter.GetAllBrokers() |> Seq.head
                let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = x.Metadata; Offset = x.Offset }) |> Seq.toArray
                let request = new OffsetCommitV0Request(consumerGroup, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
                broker.Send(request)
            let response = refreshMetadataOnException innerCommit
            if response.Topics |> Seq.exists (fun t -> t.Partitions |> Seq.exists (fun p -> p.ErrorCode.IsError())) then
                    invalidOp (sprintf "Got an error while commiting offsets. Response was %A" response)
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// Offset manager for version 1. This commits and fetches offset to/from Kafka broker.
type ConsumerOffsetManagerV1(brokerSeeds, topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let coordinatorDictionary = new ConcurrentDictionary<string, Broker>()
    let partitions = new ConcurrentDictionary<_, _>()
    let updatePartitions (brokers : Broker seq) =
        brokers
            |> Seq.map (fun x -> x.LeaderFor)
            |> Seq.concat
            |> Seq.filter (fun x -> x.TopicName = topicName)
            |> Seq.map (fun x -> x.PartitionIds)
            |> Seq.concat
            |> Seq.iter (fun x -> if partitions.ContainsKey(x) then () else partitions.TryAdd(x, null) |> ignore)
        brokers
            |> Seq.map (fun x -> x.LeaderFor)
            |> Seq.concat
            |> Seq.filter (fun x -> x.TopicName = topicName)
            |> Seq.map (fun x -> x.PartitionIds)
            |> Seq.concat
            |> Seq.filter (partitions.ContainsKey >> not)
            |> Seq.iter (partitions.TryRemove >> ignore)
    let refreshMetadataOnException f =
        try
            f()
        with
        | _ ->
            brokerRouter.RefreshMetadata()
            f()
    let getOffsetCoordinator consumerGroup =
        let send () =
            let allBrokers = brokerRouter.GetAllBrokers()
            let broker = allBrokers |> Seq.head
            let request = new ConsumerMetadataRequest(consumerGroup)
            let response = broker.Send(request)
            allBrokers |> Seq.filter (fun x -> x.NodeId = response.CoordinatorId) |> Seq.exactlyOne
        refreshMetadataOnException send
    let (|HasError|) errorCode (partitions : OffsetFetchResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)
    let (|HasCommitError|) errorCode (partitions : OffsetCommitResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)
    do
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updatePartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updatePartitions)
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV1(brokerSeeds, topicName, new BrokerRouter(tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
            if disposed then invalidOp "Offset manager has been disposed"
            let rec innerFetch () =
                let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
                let request = new OffsetFetchRequest(consumerGroup, [| { OffsetFetchRequestTopic.Name = topicName; Partitions = partitions.Keys |> Seq.toArray } |], int16 1)
                let response = coordinator.Send(request)
                let partitions =
                    response.Topics
                    |> Seq.filter (fun x -> x.Name = topicName)
                    |> Seq.map (fun x -> x.Partitions)
                    |> Seq.concat
                    |> Seq.toArray
                match partitions with
                | HasError ErrorCode.ConsumerCoordinatorNotAvailable true | HasError ErrorCode.OffsetLoadInProgress true -> innerFetch()
                | HasError ErrorCode.NotCoordinatorForConsumer true ->
                    coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
                    innerFetch()
                | _ ->
                    partitions
                    |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
                    |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = ""; Offset = x.Offset })
                    |> Seq.toArray
            refreshMetadataOnException innerFetch
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            let rec innerCommit () =
                let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
                let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV1Partition.Id = x.PartitionId; Metadata = ""; Offset = x.Offset; TimeStamp = int64 0 }) |> Seq.toArray
                let request = new OffsetCommitV1Request(consumerGroup, -1, "", [| { OffsetCommitRequestV1Topic.Name = topicName; Partitions = partitions } |])
                let response = coordinator.Send(request)
                let partitions =
                    response.Topics
                    |> Seq.filter (fun x -> x.Name = topicName)
                    |> Seq.map (fun x -> x.Partitions)
                    |> Seq.concat
                    |> Seq.toArray
                match partitions with
                | HasCommitError ErrorCode.ConsumerCoordinatorNotAvailable true | HasCommitError ErrorCode.OffsetLoadInProgress true -> innerCommit()
                | HasCommitError ErrorCode.NotCoordinatorForConsumer true ->
                    coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
                    innerCommit()
                | _ -> ()
            refreshMetadataOnException innerCommit
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// Offset manager commiting offfsets to both Zookeeper and Kafka, but only fetches from Zookeeper. Used when migrating from Zookeeper to Kafka.
type ConsumerOffsetManagerDualCommit(brokerSeeds, topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let consumerOffsetManagerV0 = new ConsumerOffsetManagerV0(brokerSeeds, topicName, brokerRouter) :> IConsumerOffsetManager
    let consumerOffsetManagerV1 = new ConsumerOffsetManagerV1(brokerSeeds, topicName, brokerRouter) :> IConsumerOffsetManager
    new (brokerSeeds, topicName, tcpTimeout : int) = new ConsumerOffsetManagerDualCommit(brokerSeeds, topicName, new BrokerRouter(tcpTimeout))
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
            if disposed then invalidOp "Offset manager has been disposed"
            consumerOffsetManagerV0.Fetch(consumerGroup)
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            consumerOffsetManagerV0.Commit(consumerGroup, offsets)
            consumerOffsetManagerV1.Commit(consumerGroup, offsets)
    interface IDisposable with
        member __.Dispose() =
            if not disposed then
                consumerOffsetManagerV0.Dispose()
                consumerOffsetManagerV1.Dispose()
                disposed <- true

/// Offset manager commiting offfsets to both Zookeeper and Kafka, but only fetches from Zookeeper. Used when migrating from Zookeeper to Kafka.
type DisabledConsumerOffsetManager() =
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(_) = [||]
        /// Commit offset for the specified topic and partitions
        member __.Commit(_, _) = ()
    interface IDisposable with
        member __.Dispose() = ()

type IConsumer =
    abstract member Consume : System.Threading.CancellationToken -> IEnumerable<Message>
    abstract member GetOffsets : unit -> PartitionOffset array
    abstract member SetOffsets : PartitionOffset array -> unit
    abstract member OffsetManager : IConsumerOffsetManager

/// Offset storage type
type OffsetStorage =
    /// Do not use any offset storage
    | None = 0
    /// Store offsets on the Zookeeper
    | Zookeeper = 1
    /// Store offsets on the Kafka brokers
    | Kafka = 2
    /// Store offsets both on the Zookeeper and Kafka brokers
    | DualCommit = 3

/// Consumer options
type ConsumerOptions() =
    /// The timeout for sending and receiving TCP data in milliseconds. Default value is 10000.
    member val TcpTimeout = 10000 with get, set
    /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued. Default value is 5000.
    member val MaxWaitTime = 5000 with get, set
    /// This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0 the server will always respond immediately,
    /// however if there is no new data since their last request they will just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has
    // at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for
    /// reading only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
    /// Default value is 1024.
    member val MinBytes = 1024 with get, set
    /// The maximum bytes to include in the message set for a partition. This helps bound the size of the response. Default value is 5120.
    member val MaxBytes = 1024 * 5 with get, set
    /// Indicates how offsets should be stored
    member val OffsetStorage = OffsetStorage.Zookeeper with get, set

[<NoEquality;NoComparison>]
type MessageWithOffset =
    {
        Offset : Offset;
        Message : Message;
    }

/// High level kafka consumer.
type Consumer(brokerSeeds, topicName, consumerOptions : ConsumerOptions, partitionWhitelist : Id array, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let offsetManager : IConsumerOffsetManager =
        match consumerOptions.OffsetStorage with
        | OffsetStorage.Zookeeper -> (new ConsumerOffsetManagerV0(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.Kafka -> (new ConsumerOffsetManagerV1(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.DualCommit -> (new ConsumerOffsetManagerDualCommit(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
        | _ -> (new DisabledConsumerOffsetManager()) :> IConsumerOffsetManager
    let partitionOffsets = new ConcurrentDictionary<Id, Offset>()
    let updateTopicPartitions (brokers : Broker seq) =
        brokers
        |> Seq.map (fun x -> x.LeaderFor)
        |> Seq.concat
        |> Seq.filter (fun x -> x.TopicName = topicName)
        |> Seq.map (fun x ->
            match partitionWhitelist with
            | null | [||] -> x.PartitionIds
            | _ -> Set.intersect (Set.ofArray x.PartitionIds) (Set.ofArray partitionWhitelist) |> Set.toArray)
        |> Seq.concat
        |> Seq.iter (fun id -> partitionOffsets.AddOrUpdate(id, new Func<Id, Offset>(fun _ -> int64 0), fun _ value -> value) |> ignore)
    do
        brokerRouter.Error.Add(fun x -> dprintfn "%A" x)
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
    new (brokerSeeds, topicName, consumerOptions) = new Consumer(brokerSeeds, topicName, consumerOptions, [||])
    new (brokerSeeds, topicName) = new Consumer(brokerSeeds, topicName, new ConsumerOptions(), [||])
    new (brokerSeeds, topicName, consumerOptions, partitionWhitelist) = new Consumer(brokerSeeds, topicName, consumerOptions, partitionWhitelist, new BrokerRouter(consumerOptions.TcpTimeout))
    /// Gets the offset manager
    member __.OffsetManager = offsetManager
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable.
    member self.Consume(cancellationToken : System.Threading.CancellationToken) =
        if disposed then invalidOp "Consumer has been disposed"
        self.ConsumeWithMetadata(cancellationToken)
        |> Seq.map (fun x -> x.Message)
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable. Also returns offset of the message.
    member __.ConsumeWithMetadata(cancellationToken : System.Threading.CancellationToken) =
        if disposed then invalidOp "Consumer has been disposed"
        let blockingCollection = new System.Collections.Concurrent.BlockingCollection<_>()
        let handleOffsetOutOfRangeError (broker : Broker) partitionId =
            let request = new OffsetRequest(-1, [| { Name = topicName; Partitions = [| { Id = partitionId; MaxNumberOfOffsets = 1; Time = int64 -2 } |] } |])
            let response = broker.Send(request)
            let earliestOffset = 
                response.Topics
                |> Seq.filter (fun x -> x.Name = topicName)
                |> Seq.map (fun x -> x.Partitions)
                |> Seq.concat
                |> Seq.filter (fun x -> x.Id = partitionId && x.ErrorCode.IsSuccess())
                |> Seq.map (fun x -> x.Offsets)
                |> Seq.concat
                |> Seq.min
            partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> earliestOffset), fun _ _ -> earliestOffset) |> ignore
        let decompressMessageSets (messageSets : MessageSet array) =
            let innerDecompress (messageSet : MessageSet) =
                match messageSet.Message.CompressionCodec with
                | Gzip -> invalidOp "GZip compression not supported yet"
                | Snappy -> SnappyCompression.Decode(messageSet)
                | None -> [| messageSet |]
            messageSets
            |> Seq.map innerDecompress
            |> Seq.concat
        let rec innerConsumer partitionId =
            async {
                let (_, offset) = partitionOffsets.TryGetValue(partitionId)
                let request = new FetchRequest(-1, consumerOptions.MaxWaitTime, consumerOptions.MinBytes, [| { Name = topicName; Partitions = [| { FetchOffset = offset; Id = partitionId; MaxBytes = consumerOptions.MaxBytes } |] } |])
                let broker = brokerRouter.GetBroker(topicName, partitionId)
                let rec trySend (broker : Broker) attempt =
                    try
                        broker.Send(request)
                    with
                    | e ->
                        dprintfn "Got exception while sending request %s" e.Message
                        if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
                        else
                            brokerRouter.RefreshMetadata()
                            let newBroker = brokerRouter.GetBroker(topicName, partitionId)
                            trySend newBroker (attempt + 1)
                let response = trySend broker 0
                let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
                match partitionResponse.ErrorCode with
                | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable ->
                    partitionResponse.MessageSets
                        |> decompressMessageSets
                        |> Seq.iter (fun x -> blockingCollection.Add({ Message = x.Message; Offset = x.Offset }))
                    if partitionResponse.MessageSets |> Seq.isEmpty |> not then
                        let nextOffset = (partitionResponse.MessageSets |> Seq.map (fun x -> x.Offset) |> Seq.max) + int64 1
                        partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> nextOffset), fun _ _ -> nextOffset) |> ignore
                | ErrorCode.NotLeaderForPartition ->
                    brokerRouter.RefreshMetadata()
                    return! innerConsumer partitionId
                | ErrorCode.OffsetOutOfRange ->
                    handleOffsetOutOfRangeError broker partitionId
                | _ -> invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)
                if cancellationToken.IsCancellationRequested then () else return! innerConsumer partitionId
            }
        let asyncs =
            partitionOffsets.Keys
            |> Seq.map innerConsumer
            |> Async.Parallel
            |> Async.Ignore
        Async.Start(asyncs, cancellationToken)
        blockingCollection.GetConsumingEnumerable(cancellationToken)
    /// Get the current consumer offsets
    member __.GetOffsets() =
        if disposed then invalidOp "Consumer has been disposed"
        partitionOffsets |> Seq.map (fun x -> { PartitionId = x.Key; Offset = x.Value; Metadata = String.Empty }) |> Seq.toArray
    /// Sets the current consumer offsets
    member __.SetOffsets(offsets) =
        if disposed then invalidOp "Consumer has been disposed"
        offsets |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
    /// Releases all connections and disposes the consumer
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IConsumer with
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.GetOffsets() =
            self.GetOffsets()
        member self.SetOffsets(offsets) =
            self.SetOffsets(offsets)
        member self.OffsetManager = self.OffsetManager
    interface IDisposable with
        member self.Dispose() = self.Dispose()
