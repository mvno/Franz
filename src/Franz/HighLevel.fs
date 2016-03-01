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
    abstract member SendMessage : string * string * RequiredAcks * int -> unit
    abstract member SendMessage : string * string -> unit
    abstract member SendMessages : string * string array -> unit
    abstract member SendMessages : string * string array * RequiredAcks * int -> unit
    abstract member SendMessage : string * string * string * RequiredAcks * int -> unit
    abstract member SendMessage : string * string * string -> unit
    abstract member SendMessages : string * string * string array -> unit
    abstract member SendMessages : string * string * string array * RequiredAcks * int -> unit

/// High level kafka producer
type Producer(brokerSeeds, brokerRouter : BrokerRouter, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) =
    let mutable disposed = false

    let compressMessages (messages : string array) =
        let messageSets = messages |> Array.map (fun x -> MessageSet.Create(int64 -1, int8 0, null, Encoding.UTF8.GetBytes(x)))
        match compressionCodec with
        | CompressionCodec.None -> messageSets
        | CompressionCodec.Gzip -> GzipCompression.Encode(messageSets)
        | CompressionCodec.Snappy -> SnappyCompression.Encode(messageSets)
        | x -> failwithf "Unsupported compression codec %A" x

    let rec trySend (broker : Broker) attempt request topicName partitionId =
        try
            broker.Send(request)
        with
        | e ->
            dprintfn "Got exception while sending request %s" e.Message
            if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
            else
                brokerRouter.RefreshMetadata()
                let newBroker = brokerRouter.GetBroker(topicName, partitionId)
                trySend newBroker (attempt + 1) request topicName partitionId

    let rec innerSend key messages topicName requiredAcks brokerProcessingTimeout =
        let messageSets = messages |> compressMessages
        let partitionId = partitionSelector.Invoke(topicName, key)
        let partitions = { PartitionProduceRequest.Id = partitionId; MessageSets = messageSets; TotalMessageSetsSize = messageSets |> Seq.sumBy (fun x -> x.MessageSetSize) }
        let topic = { TopicProduceRequest.Name = topicName; Partitions = [| partitions |] }
        let request = new ProduceRequest(requiredAcks, brokerProcessingTimeout, [| topic |])
        let broker = brokerRouter.GetBroker(topicName, partitionId)
        let response = trySend broker 0 request topicName partitionId
        let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
        match partitionResponse.ErrorCode with
        | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable -> ()
        | ErrorCode.NotLeaderForPartition ->
            brokerRouter.RefreshMetadata()
            innerSend key messages topicName requiredAcks brokerProcessingTimeout
        | _ -> invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)

    do
        brokerRouter.Error.Add(fun x -> dprintfn "%A" x)
        brokerRouter.Connect(brokerSeeds)
    new (brokerSeeds, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, 10000, partitionSelector)
    new (brokerSeeds, tcpTimeout : int, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, new BrokerRouter(tcpTimeout), partitionSelector)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, new BrokerRouter(tcpTimeout), compressionCodec, partitionSelector)
    new (brokerSeeds, brokerRouter : BrokerRouter, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, brokerRouter, CompressionCodec.None, partitionSelector)
    /// Sends a message to the specified topic
    member self.SendMessages(topicName, key, message) =
        self.SendMessages(topicName, key, message, RequiredAcks.LocalLog, 500)
    /// Sends a message to the specified topic
    member __.SendMessages(topicName, key, messages : string array, requiredAcks, brokerProcessingTimeout) =
        if disposed then invalidOp "Producer has been disposed"
        innerSend key messages topicName requiredAcks brokerProcessingTimeout
    /// Get all available brokers
    member __.GetAllBrokers() =
        brokerRouter.GetAllBrokers()
    /// Releases all connections and disposes the producer
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IProducer with
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, null, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, message) =
            self.SendMessages(topicName, null, [| message |])
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, null, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, messages) = 
            self.SendMessages(topicName, null, messages)
        member self.SendMessage(topicName, key, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, key, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, key, message) =
            self.SendMessages(topicName, key, [| message |])
        member self.SendMessages(topicName, key, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, key, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, key, messages) = 
            self.SendMessages(topicName, key, messages)
    interface IDisposable with
        member self.Dispose() = self.Dispose()

type RoundRobinProducer(brokerSeeds, brokerRouter : BrokerRouter, compressionCodec : CompressionCodec, partitionWhiteList : Id array) =
    let mutable producer = None
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

    let getNextPartitionId topicName _ =
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

    do
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
        producer <- Some <| new Producer(brokerSeeds, brokerRouter, compressionCodec, new Func<string, string, Id>(getNextPartitionId))

    new (brokerSeeds) = new RoundRobinProducer(brokerSeeds, 10000)
    new (brokerSeeds, tcpTimeout : int) = new RoundRobinProducer(brokerSeeds, new BrokerRouter(tcpTimeout))
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec) = new RoundRobinProducer(brokerSeeds, new BrokerRouter(tcpTimeout), compressionCodec, null)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionWhiteList : Id array) = new RoundRobinProducer(brokerSeeds, new BrokerRouter(tcpTimeout), compressionCodec, partitionWhiteList)
    new (brokerSeeds, brokerRouter : BrokerRouter) = new RoundRobinProducer(brokerSeeds, brokerRouter, CompressionCodec.None, null)

    /// Releases all connections and disposes the producer
    member __.Dispose() =
        producer.Value.Dispose()

    /// Sends a message to the specified topic
    member __.SendMessages(topicName, key, message) =
        producer.Value.SendMessages(topicName, key, message, RequiredAcks.LocalLog, 500)
    /// Sends a message to the specified topic
    member __.SendMessages(topicName, key, messages : string array, requiredAcks, brokerProcessingTimeout) =
        producer.Value.SendMessages(topicName, key, messages, requiredAcks, brokerProcessingTimeout)

    interface IProducer with
        member self.SendMessage(topicName, key, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, key, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, key, message) =
            self.SendMessages(topicName, key, [| message |])
        member self.SendMessages(topicName, key, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, key, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, key, messages) =
            self.SendMessages(topicName, key, messages)
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, null, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, message) =
            self.SendMessages(topicName, null, [| message |])
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, null, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, messages) =
            self.SendMessages(topicName, null, messages)
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

    let innerFetch consumerGroup =
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

    let innerCommit offsets consumerGroup =
        let broker = brokerRouter.GetAllBrokers() |> Seq.head
        let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = x.Metadata; Offset = x.Offset }) |> Seq.toArray
        let request = new OffsetCommitV0Request(consumerGroup, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
        broker.Send(request)

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
            refreshMetadataOnException (fun () -> innerFetch consumerGroup)
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            let response = refreshMetadataOnException (fun () -> innerCommit offsets consumerGroup)
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
    let send consumerGroup =
        let allBrokers = brokerRouter.GetAllBrokers()
        let broker = allBrokers |> Seq.head
        let request = new ConsumerMetadataRequest(consumerGroup)
        let response = broker.Send(request)
        allBrokers |> Seq.filter (fun x -> x.NodeId = response.CoordinatorId) |> Seq.exactlyOne
    let getOffsetCoordinator consumerGroup =
        refreshMetadataOnException (fun () -> send consumerGroup)
    let (|HasError|) errorCode (partitions : OffsetFetchResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)
    let (|HasCommitError|) errorCode (partitions : OffsetCommitResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)

    let rec innerFetch consumerGroup =
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
        | HasError ErrorCode.ConsumerCoordinatorNotAvailable true | HasError ErrorCode.GroupLoadInProgressCode true -> innerFetch consumerGroup
        | HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerFetch consumerGroup
        | _ ->
            partitions
            |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
            |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = ""; Offset = x.Offset })
            |> Seq.toArray

    let rec innerCommit consumerGroup offsets =
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
        | HasCommitError ErrorCode.ConsumerCoordinatorNotAvailable true | HasCommitError ErrorCode.GroupLoadInProgressCode true -> innerCommit consumerGroup offsets
        | HasCommitError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerCommit consumerGroup offsets
        | _ ->
            let errorCode = (partitions |> Seq.tryFind (fun x -> x.ErrorCode <> ErrorCode.NoError))
            match errorCode with
            | Some x -> dprintfn "Got error '%A' while commiting offset" x.ErrorCode
            | None -> ()

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
            refreshMetadataOnException (fun () -> innerFetch consumerGroup)
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            refreshMetadataOnException (fun () -> innerCommit consumerGroup offsets)
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// Offset manager for version 2. This commits and fetches offset to/from Kafka broker.
type ConsumerOffsetManagerV2(brokerSeeds, topicName, brokerRouter : BrokerRouter) =
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
    let send consumerGroup =
        let allBrokers = brokerRouter.GetAllBrokers()
        let broker = allBrokers |> Seq.head
        let request = new ConsumerMetadataRequest(consumerGroup)
        let response = broker.Send(request)
        allBrokers |> Seq.filter (fun x -> x.NodeId = response.CoordinatorId) |> Seq.exactlyOne
    let getOffsetCoordinator consumerGroup =
        refreshMetadataOnException (fun () -> send consumerGroup)
    let (|HasError|) errorCode (partitions : OffsetFetchResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)
    let (|HasCommitError|) errorCode (partitions : OffsetCommitResponsePartition array) =
        partitions |> Seq.exists (fun x -> x.ErrorCode = errorCode)

    let rec innerFetch consumerGroup =
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
        | HasError ErrorCode.ConsumerCoordinatorNotAvailable true | HasError ErrorCode.GroupLoadInProgressCode true -> innerFetch consumerGroup
        | HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerFetch consumerGroup
        | _ ->
            partitions
            |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
            |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = ""; Offset = x.Offset })
            |> Seq.toArray

    let rec innerCommit consumerGroup offsets =
        let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
        let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = ""; Offset = x.Offset }) |> Seq.toArray
        let request = new OffsetCommitV2Request(consumerGroup, -1, "", -1L, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
        let response = coordinator.Send(request)
        let partitions =
            response.Topics
            |> Seq.filter (fun x -> x.Name = topicName)
            |> Seq.map (fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray
        match partitions with
        | HasCommitError ErrorCode.ConsumerCoordinatorNotAvailable true | HasCommitError ErrorCode.GroupLoadInProgressCode true -> innerCommit consumerGroup offsets
        | HasCommitError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerCommit consumerGroup offsets
        | _ ->
            let errorCode = (partitions |> Seq.tryFind (fun x -> x.ErrorCode <> ErrorCode.NoError))
            match errorCode with
            | Some x -> dprintfn "Got error '%A' while commiting offset" x.ErrorCode
            | None -> ()

    do
        brokerRouter.Connect(brokerSeeds)
        brokerRouter.GetAllBrokers() |> updatePartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updatePartitions)
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV2(brokerSeeds, topicName, new BrokerRouter(tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(consumerGroup) =
            if disposed then invalidOp "Offset manager has been disposed"
            refreshMetadataOnException (fun () -> innerFetch consumerGroup)
        /// Commit offset for the specified topic and partitions
        member __.Commit(consumerGroup, offsets) =
            if disposed then invalidOp "Offset manager has been disposed"
            refreshMetadataOnException (fun () -> innerCommit consumerGroup offsets)
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

[<NoEquality;NoComparison>]
type MessageWithMetadata =
    {
        Offset : Offset;
        Message : Message;
        PartitionId : Id;
    }

type IConsumer =
    abstract member Consume : System.Threading.CancellationToken -> IEnumerable<Message>
    abstract member ConsumeWithMetadata : System.Threading.CancellationToken -> IEnumerable<MessageWithMetadata>
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

module private OffsetHandling =
    let createOffsetManager (brokerSeeds : EndPoint seq) (topicName : string) (brokerRouter : BrokerRouter) offsetStorage =
        match offsetStorage with
            | OffsetStorage.Zookeeper -> (new ConsumerOffsetManagerV0(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
            | OffsetStorage.Kafka -> (new ConsumerOffsetManagerV1(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
            | OffsetStorage.DualCommit -> (new ConsumerOffsetManagerDualCommit(brokerSeeds, topicName, brokerRouter)) :> IConsumerOffsetManager
            | _ -> (new DisabledConsumerOffsetManager()) :> IConsumerOffsetManager

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

module private ConsumerHandling =
    let decompressMessageSet (messageSet : MessageSet) =
        match messageSet.Message.CompressionCodec with
        | CompressionCodec.Gzip -> GzipCompression.Decode(messageSet)
        | CompressionCodec.Snappy -> SnappyCompression.Decode(messageSet)
        | CompressionCodec.None -> [ messageSet ]
        | x -> failwithf "Unknown compression codec %A" x

    let decompressMessageSets (messageSets : MessageSet seq) =
        messageSets
        |> Seq.map decompressMessageSet
        |> Seq.concat

    let handleOffsetOutOfRangeError (broker : Broker) (partitionId : Id) (topicName : string) =
        let request = new OffsetRequest(-1, [| { Name = topicName; Partitions = [| { Id = partitionId; MaxNumberOfOffsets = 1; Time = int64 -2 } |] } |])
        let response = broker.Send(request)
        response.Topics
        |> Seq.filter (fun x -> x.Name = topicName)
        |> Seq.map (fun x -> x.Partitions)
        |> Seq.concat
        |> Seq.filter (fun x -> x.Id = partitionId && x.ErrorCode.IsSuccess())
        |> Seq.map (fun x -> x.Offsets)
        |> Seq.concat
        |> Seq.min

    let rec trySendToBroker topicName attempt request partitionId (brokerRouter : BrokerRouter) (broker : Broker) =
        try
            broker.Send(request)
        with
        | e ->
            dprintfn "Got exception while sending request %s" e.Message
            if attempt > 0 then raise (InvalidOperationException("Got exception while sending request", e))
            else
                brokerRouter.RefreshMetadata()
                brokerRouter.GetBroker(topicName, partitionId)
                |> trySendToBroker topicName (attempt + 1) request partitionId brokerRouter

    let rec consumeInChunks partitionId (maxBytes : int option) (partitionOffsets : ConcurrentDictionary<_, _>) (consumerOptions : ConsumerOptions) topicName (brokerRouter : BrokerRouter) =
        async {
            try
                let (_, offset) = partitionOffsets.TryGetValue(partitionId)
                let request = new FetchRequest(-1, consumerOptions.MaxWaitTime, consumerOptions.MinBytes, [| { Name = topicName; Partitions = [| { FetchOffset = offset; Id = partitionId; MaxBytes = defaultArg maxBytes consumerOptions.MaxBytes } |] } |])
                let broker = brokerRouter.GetBroker(topicName, partitionId)
                let response = broker |> trySendToBroker topicName 0 request partitionId brokerRouter
                let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
                match partitionResponse.ErrorCode with
                | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable ->
                    let messages =
                        partitionResponse.MessageSets
                        |> decompressMessageSets
                        |> Seq.map (fun x -> { Message = x.Message; Offset = x.Offset; PartitionId = partitionId })
                    if partitionResponse.MessageSets |> Seq.isEmpty |> not then
                        let nextOffset = (partitionResponse.MessageSets |> Seq.map (fun x -> x.Offset) |> Seq.max) + int64 1
                        partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> nextOffset), fun _ _ -> nextOffset) |> ignore
                    return messages
                | ErrorCode.NotLeaderForPartition ->
                    brokerRouter.RefreshMetadata()
                    return! consumeInChunks partitionId maxBytes partitionOffsets consumerOptions topicName brokerRouter
                | ErrorCode.OffsetOutOfRange ->
                    let earliestOffset = handleOffsetOutOfRangeError broker partitionId topicName
                    partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> earliestOffset), fun _ _ -> earliestOffset) |> ignore
                    return! consumeInChunks partitionId maxBytes partitionOffsets consumerOptions topicName brokerRouter
                | _ ->
                    invalidOp (sprintf "Received broker error: %A" partitionResponse.ErrorCode)
                    return Seq.empty<_>
            with
            | :? BufferOverflowException as e ->
                dprintfn "%s. Temporarily increasing fetch size" e.Message
                let increasedFetchSize = (defaultArg maxBytes consumerOptions.MaxBytes) * 2
                return! consumeInChunks partitionId (Some increasedFetchSize) partitionOffsets consumerOptions topicName brokerRouter
            | e ->
                dprintfn "Got exception %s. Retrying in 5 seconds." e.Message
                do! Async.Sleep 5000
                return Seq.empty<_>
        }

/// High level kafka consumer.
type Consumer(brokerSeeds, topicName, consumerOptions : ConsumerOptions, partitionWhitelist : Id array, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let offsetManager = consumerOptions.OffsetStorage |> OffsetHandling.createOffsetManager brokerSeeds topicName brokerRouter
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
        let rec consume() =
            async {
                let! messagesFromAllPartitions =
                    partitionOffsets.Keys
                    |> Seq.map (fun x -> async { return! ConsumerHandling.consumeInChunks x None partitionOffsets consumerOptions topicName brokerRouter })
                    |> Async.Parallel
                messagesFromAllPartitions
                |> Seq.concat
                |> Seq.iter (fun x -> blockingCollection.Add(x))
                return! consume()
            }
        Async.Start(consume(), cancellationToken)
        blockingCollection.GetConsumingEnumerable(cancellationToken)
    /// Get the current consumer offsets
    member __.GetOffsets() =
        if disposed then invalidOp "Consumer has been disposed"
        partitionOffsets |> Seq.map (fun x -> { PartitionId = x.Key; Offset = x.Value; Metadata = String.Empty }) |> Seq.toArray
    /// Sets the current consumer offsets
    member __.SetOffsets(offsets : PartitionOffset seq) =
        if disposed then invalidOp "Consumer has been disposed"
        if partitionWhitelist <> null then
            offsets
            |> Seq.filter (fun x -> partitionWhitelist |> Seq.exists (fun y -> y = x.PartitionId))
            |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
        else
            offsets
            |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
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
        member self.ConsumeWithMetadata(cancellationToken) =
            self.ConsumeWithMetadata(cancellationToken)
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// High level kafka consumer, consuming messages in chunks defined by MaxBytes, MinBytes and MaxWaitTime in the consumer options. Each call to the consume functions,
/// will provide a new chunk of messages. If no messages are available an empty sequence will be returned.
type ChunkedConsumer(brokerSeeds, topicName, consumerOptions : ConsumerOptions, partitionWhitelist : Id array, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let offsetManager = consumerOptions.OffsetStorage |> OffsetHandling.createOffsetManager brokerSeeds topicName brokerRouter
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
    new (brokerSeeds, topicName, consumerOptions) = new ChunkedConsumer(brokerSeeds, topicName, consumerOptions, [||])
    new (brokerSeeds, topicName) = new ChunkedConsumer(brokerSeeds, topicName, new ConsumerOptions(), [||])
    new (brokerSeeds, topicName, consumerOptions, partitionWhitelist) = new ChunkedConsumer(brokerSeeds, topicName, consumerOptions, partitionWhitelist, new BrokerRouter(consumerOptions.TcpTimeout))
    /// Gets the offset manager
    member __.OffsetManager = offsetManager
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable.
    member self.Consume(cancellationToken : System.Threading.CancellationToken) =
        if disposed then invalidOp "Consumer has been disposed"
        self.ConsumeWithMetadata(cancellationToken)
        |> Seq.map (fun x -> x.Message)
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable. Also returns offset of the message.
    member __.ConsumeWithMetadata(_) =
        if disposed then invalidOp "Consumer has been disposed"
        partitionOffsets.Keys
        |> Seq.map (fun x -> async { return! ConsumerHandling.consumeInChunks x None partitionOffsets consumerOptions topicName brokerRouter })
        |> Async.Parallel
        |> Async.RunSynchronously
        |> Seq.concat
    /// Get the current consumer offsets
    member __.GetOffsets() =
        if disposed then invalidOp "Consumer has been disposed"
        partitionOffsets |> Seq.map (fun x -> { PartitionId = x.Key; Offset = x.Value; Metadata = String.Empty }) |> Seq.toArray
    /// Sets the current consumer offsets
    member __.SetOffsets(offsets : PartitionOffset seq) =
        if disposed then invalidOp "Consumer has been disposed"
        if partitionWhitelist <> null then
            offsets
            |> Seq.filter (fun x -> partitionWhitelist |> Seq.exists (fun y -> y = x.PartitionId))
            |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
        else
            offsets
            |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)
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
        member self.ConsumeWithMetadata(cancellationToken) =
            self.ConsumeWithMetadata(cancellationToken)
    interface IDisposable with
        member self.Dispose() = self.Dispose()
