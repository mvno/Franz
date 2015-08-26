namespace Franz.HighLevel

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Text
open Franz
open Franz.Internal
open System.Collections.Concurrent

module Seq =
    /// Helper function to convert a sequence to a List<T>
    let toBclList (x : 'a seq) =
        new List<_>(x)

type PartiontionIds = List<Id>
type NextPartitionId = Id
type TopicPartitions = Dictionary<string, (PartiontionIds * NextPartitionId)>

type IProducer =
    abstract member SendMessage : string * string * RequiredAcks * int -> unit

/// High level kafka producer
type Producer(brokerSeeds, tcpTimeout) =
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
    let lowLevelRouter = new BrokerRouter(tcpTimeout)
    do
        lowLevelRouter.Error.Add(fun x -> dprintfn "%A" x)
        lowLevelRouter.Connect(brokerSeeds)
        lowLevelRouter.GetAllBrokers() |> updateTopicPartitions
        lowLevelRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
    /// Sends a message to the specified topic
    member __.SendMessage(topicName, message : string, [<Optional;DefaultParameterValue(RequiredAcks.LocalLog)>]requiredAcks, [<Optional;DefaultParameterValue(500)>]brokerProcessingTimeout) =
        let rec innerSend() =
            let messageSet =
                let value = Encoding.UTF8.GetBytes(message)
                MessageSet.Create(int64 -1, int8 0, null, value)
            let partitionId =
                let success, result = topicPartitions.TryGetValue(topicName)
                if (not success) then
                    lowLevelRouter.GetBroker(topicName, 0) |> ignore
                    0
                else
                    let (ids, id) = result
                    let nextId = if id = (ids |> Seq.max) then ids |> Seq.min else ids |> Seq.find (fun x -> x > id)
                    topicPartitions.[topicName] <- (ids, nextId)
                    id
            let partitions = { PartitionProduceRequest.Id = partitionId; MessageSet = messageSet; MessageSetSize = messageSet.MessageSetSize }
            let topic = { TopicProduceRequest.Name = topicName; Partitions = [| partitions |] }
            let request = new ProduceRequest(requiredAcks, brokerProcessingTimeout, [| topic |])
            let broker = lowLevelRouter.GetBroker(topicName, partitionId)
            let rec trySend (broker : Broker) attempt =
                try
                    broker.Send(request)
                with
                | e ->
                    dprintfn "Got exception while sending request %s" e.Message
                    if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
                    else
                        lowLevelRouter.RefreshMetadata()
                        let newBroker = lowLevelRouter.GetBroker(topicName, partitionId)
                        trySend newBroker (attempt + 1)
            let response = trySend broker 0
            let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
            match partitionResponse.ErrorCode with
            | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable -> ()
            | ErrorCode.NotLeaderForPartition ->
                lowLevelRouter.RefreshMetadata()
                innerSend()
            | _ -> invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)
        innerSend()
    /// Get all available brokers
    member __.GetAllBrokers() =
        lowLevelRouter.GetAllBrokers()
    interface IProducer with
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout)

/// Information about offsets
type PartitionOffset = { PartitionId : Id; Offset : Offset; Metadata : string }

/// Interface for offset managers
type IConsumerOffsetManager =
    /// Fetch offset for the specified topic and partitions
    abstract member Fetch : string -> PartitionOffset array
    /// Commit offset for the specified topic and partitions
    abstract member Commit : string * PartitionOffset seq -> unit

/// Offset manager for version 0. This commits and fetches offset to/from Zookeeper instances.
type ConsumerOffsetManagerV0(brokerSeeds, topicName, tcpTimeout) =
    let lowLevelRouter = new BrokerRouter(tcpTimeout)
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
            |> Seq.filter (fun x -> partitions.ContainsKey(x) |> not)
            |> Seq.iter (fun x -> partitions.TryRemove(x) |> ignore)
    let refreshMetadataOnException f =
        try
            f()
        with
        | _ ->
            lowLevelRouter.RefreshMetadata()
            f()
    do
        lowLevelRouter.Connect(brokerSeeds)
        lowLevelRouter.GetAllBrokers() |> updatePartitions
        lowLevelRouter.MetadataRefreshed.Add(fun x -> x |> updatePartitions)
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
            let innerFetch () =
                let broker = lowLevelRouter.GetAllBrokers() |> Seq.head
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
            let innerCommit () =
                let broker = lowLevelRouter.GetAllBrokers() |> Seq.head
                let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = x.Metadata; Offset = x.Offset }) |> Seq.toArray
                let request = new OffsetCommitV0Request(consumerGroup, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
                broker.Send(request)
            let response = refreshMetadataOnException innerCommit
            if response.Topics |> Seq.exists (fun t -> t.Partitions |> Seq.exists (fun p -> p.ErrorCode.IsError())) then
                    invalidOp (sprintf "Got an error while commiting offsets. Response was %A" response)

/// Offset manager for version 1. This commits and fetches offset to/from Kafka broker.
type ConsumerOffsetManagerV1(brokerSeeds, topicName, tcpTimeout) =
    let coordinatorDictionary = new ConcurrentDictionary<string, Broker>()
    let lowLevelRouter = new BrokerRouter(tcpTimeout)
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
            |> Seq.filter (fun x -> partitions.ContainsKey(x) |> not)
            |> Seq.iter (fun x -> partitions.TryRemove(x) |> ignore)
    let refreshMetadataOnException f =
        try
            f()
        with
        | _ ->
            lowLevelRouter.RefreshMetadata()
            f()
    let getOffsetCoordinator consumerGroup =
        let send () =
            let allBrokers = lowLevelRouter.GetAllBrokers()
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
        lowLevelRouter.Connect(brokerSeeds)
        lowLevelRouter.GetAllBrokers() |> updatePartitions
        lowLevelRouter.MetadataRefreshed.Add(fun x -> x |> updatePartitions)
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
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

type IConsumer =
    abstract member Consume : System.Threading.CancellationToken -> IEnumerable<Message>
    abstract member GetOffsets : unit -> PartitionOffset array
    abstract member SetOffsets : PartitionOffset array -> unit
    abstract member OffsetManager : IConsumerOffsetManager

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

/// High level kafka consumer.
type Consumer(brokerSeeds, topicName, consumerOptions : ConsumerOptions, offsetManager : IConsumerOffsetManager) =
    let lowLevelRouter = new BrokerRouter(consumerOptions.TcpTimeout)
    let partitionOffsets = new ConcurrentDictionary<Id, Offset>()
    let updateTopicPartitions (brokers : Broker seq) =
        brokers
        |> Seq.map (fun x -> x.LeaderFor)
        |> Seq.concat
        |> Seq.filter (fun x -> x.TopicName = topicName)
        |> Seq.map (fun x -> x.PartitionIds)
        |> Seq.concat
        |> Seq.iter (fun id -> partitionOffsets.AddOrUpdate(id, new Func<Id, Offset>(fun _ -> int64 0), fun _ value -> value) |> ignore)
    do
        lowLevelRouter.Error.Add(fun x -> dprintfn "%A" x)
        lowLevelRouter.Connect(brokerSeeds)
        lowLevelRouter.GetAllBrokers() |> updateTopicPartitions
        lowLevelRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
    /// Gets the offset manager
    member __.OffsetManager = offsetManager
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable.
    member __.Consume(cancellationToken : System.Threading.CancellationToken) =
        let blockingCollection = new System.Collections.Concurrent.BlockingCollection<_>()
        let rec innerConsumer partitionId =
            async {
                let (_, offset) = partitionOffsets.TryGetValue(partitionId)
                let request = new FetchRequest(-1, 1000, 1, [| { Name = topicName; Partitions = [| { FetchOffset = offset; Id = partitionId; MaxBytes = 512 } |] } |])
                let broker = lowLevelRouter.GetBroker(topicName, partitionId)
                let rec trySend (broker : Broker) attempt =
                    try
                        broker.Send(request)
                    with
                    | e ->
                        dprintfn "Got exception while sending request %s" e.Message
                        if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
                        else
                            lowLevelRouter.RefreshMetadata()
                            let newBroker = lowLevelRouter.GetBroker(topicName, partitionId)
                            trySend newBroker (attempt + 1)
                let response = trySend broker 0
                let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
                match partitionResponse.ErrorCode with
                | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable ->
                    partitionResponse.MessageSets
                        |> Seq.iter (fun x -> blockingCollection.Add(x.Message))
                    if partitionResponse.MessageSets |> Seq.isEmpty |> not then
                        let nextOffset = (partitionResponse.MessageSets |> Seq.map (fun x -> x.Offset) |> Seq.max) + int64 1
                        partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> nextOffset), fun _ _ -> nextOffset) |> ignore
                | ErrorCode.NotLeaderForPartition ->
                    lowLevelRouter.RefreshMetadata()
                    return! innerConsumer partitionId
                | _ -> invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)
                if cancellationToken.IsCancellationRequested then () else return! innerConsumer partitionId
            }
        partitionOffsets.Keys
            |> Seq.map (fun x -> innerConsumer x)
            |> Async.Parallel
            |> Async.Ignore
            |> Async.Start
        blockingCollection.GetConsumingEnumerable(cancellationToken)
    /// Get the current consumer offsets
    member __.GetOffsets() =
        partitionOffsets |> Seq.map (fun x -> { PartitionId = x.Key; Offset = x.Value; Metadata = String.Empty }) |> Seq.toArray
    /// Sets the current consumer offsets
    member __.SetOffsets(offsets) =
        offsets |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
    interface IConsumer with
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.GetOffsets() =
            self.GetOffsets()
        member self.SetOffsets(offsets) =
            self.SetOffsets(offsets)
        member self.OffsetManager = self.OffsetManager
