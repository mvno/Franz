namespace Franz.HighLevel

open System
open System.Collections.Generic
open Franz
open System.Collections.Concurrent
open Franz.Compression
open Franz.Internal

type ErrorCommittingOffsetException (offsetManagerName : string, topic : string, consumerGroup : string, errorCodes : seq<string>) =
    inherit Exception()
    member e.Codes = errorCodes
    member e.OffsetManagerName = offsetManagerName
    member e.Topic = topic
    member e.ConsumerGroup = consumerGroup
    override e.Message = sprintf "One or more errors occoured while committing offsets (%s) for topic '%s' group '%s': %A" e.OffsetManagerName e.Topic e.ConsumerGroup e.Codes

type RequestTimedOutException() =
    inherit Exception()
    override e.Message = "Producer received RequestTimedOut on Ack from Brokers"

type RequestTimedOutRetryExceededException() =
    inherit Exception()
    override e.Message = "Producer received RequestTimedOut on Ack from Brokers to many times"

type BrokerReturnedErrorException (errorCode : ErrorCode) =
    inherit Exception()
    member e.Code = errorCode
    override e.Message = sprintf "Broker returned a response with error code: %s" (e.Code.ToString())

module Seq =
    /// Helper function to convert a sequence to a List<T>
    let toBclList (x : 'a seq) =
        new List<_>(x)

type PartiontionIds = List<Id>
type NextPartitionId = Id
type TopicPartitions = Dictionary<string, (PartiontionIds * NextPartitionId)>

type IProducer =
    inherit IDisposable
    abstract member SendMessage : string * string * RequiredAcks * int -> unit
    abstract member SendMessage : string * string -> unit
    abstract member SendMessages : string * string array -> unit
    abstract member SendMessages : string * string array * RequiredAcks * int -> unit
    abstract member SendMessage : string * string * string * RequiredAcks * int -> unit
    abstract member SendMessage : string * string * string -> unit
    abstract member SendMessages : string * string * string array -> unit
    abstract member SendMessages : string * string * string array * RequiredAcks * int -> unit

[<AbstractClass>]
type BaseProducer (brokerRouter : BrokerRouter, compressionCodec, partitionSelector : Func<string, string, Id>) =
    let mutable disposed = false

    let retryOnRequestTimedOut retrySendFunction (retryCount : int) =
        LogConfiguration.Logger.Warning.Invoke(sprintf "Producer received RequestTimedOut on Ack from Brokers, retrying (%i) with increased timeout" retryCount, RequestTimedOutException())
        if retryCount > 1 then raiseWithErrorLog(RequestTimedOutRetryExceededException())
        retrySendFunction()
    
    let rec send topicName key messages requiredAcks brokerProcessingTimeout retryCount =
        let messageSets = Compression.CompressMessages(compressionCodec, messages)
        let partitionId = partitionSelector.Invoke(topicName, key)
        let partitions = { PartitionProduceRequest.Id = partitionId; MessageSets = messageSets; TotalMessageSetsSize = messageSets |> Seq.sumBy (fun x -> x.MessageSetSize) }
        let topic = { TopicProduceRequest.Name = topicName; Partitions = [| partitions |] }
        let request = new ProduceRequest(requiredAcks, brokerProcessingTimeout, [| topic |])
        let response = brokerRouter.TrySendToBroker(topicName, partitionId, request)
        let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
        match partitionResponse.ErrorCode with
        | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable -> ()
        | ErrorCode.NotLeaderForPartition ->
            brokerRouter.RefreshMetadata()
            send topicName key messages requiredAcks brokerProcessingTimeout 0
        | ErrorCode.RequestTimedOut ->
            retryOnRequestTimedOut (fun () -> send topicName key messages requiredAcks (brokerProcessingTimeout * 2) (retryCount + 1)) retryCount
        | _ -> raiseWithErrorLog(BrokerReturnedErrorException partitionResponse.ErrorCode)

    do
        brokerRouter.Error.Add(fun x -> LogConfiguration.Logger.Fatal.Invoke(sprintf "Unhandled exception in BrokerRouter", x))
        brokerRouter.Connect()

    /// Sends a message to the specified topic
    abstract member SendMessages : string * string * string array * RequiredAcks * int -> unit

    /// Sends a message to the specified topic
    default __.SendMessages(topic, key, messages, requiredAcks, brokerProcessingTimeout) =
        let messageSets = messages |> Array.map (fun x -> MessageSet.Create(int64 -1, int8 0, System.Text.Encoding.UTF8.GetBytes(key), System.Text.Encoding.UTF8.GetBytes(x)))
        try
            send topic key messageSets requiredAcks brokerProcessingTimeout 0
        with
        | _ ->
            brokerRouter.RefreshMetadata()
            send topic key messageSets requiredAcks brokerProcessingTimeout 0

    /// Get all available brokers
    member __.GetAllBrokers() = brokerRouter.GetAllBrokers()

    /// Releases all connections and disposes the producer
    interface IDisposable with
        member __.Dispose() =
            if not disposed then
                brokerRouter.Dispose()
                disposed <- true

/// High level kafka producer
type Producer(brokerRouter : BrokerRouter, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) =
    inherit BaseProducer(brokerRouter, compressionCodec, partitionSelector)

    new (brokerSeeds, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, 10000, partitionSelector)
    new (brokerSeeds, tcpTimeout : int, partitionSelector : Func<string, string, Id>) = new Producer(new BrokerRouter(brokerSeeds, tcpTimeout), partitionSelector)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) = new Producer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, partitionSelector)
    new (brokerRouter : BrokerRouter, partitionSelector : Func<string, string, Id>) = new Producer(brokerRouter, CompressionCodec.None, partitionSelector)
    
    /// Sends a message to the specified topic
    member self.SendMessages(topicName, key, message) =
        self.SendMessages(topicName, key, message, RequiredAcks.LocalLog, 500)
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
        member self.Dispose() = (self :> IDisposable).Dispose()

type RoundRobinProducer(brokerRouter : BrokerRouter, compressionCodec : CompressionCodec, partitionWhiteList : Id array) =
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
        brokerRouter.Connect()
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)
        producer <- Some <| new Producer(brokerRouter, compressionCodec, new Func<string, string, Id>(getNextPartitionId))

    new (brokerSeeds) = new RoundRobinProducer(brokerSeeds, 10000)
    new (brokerSeeds, tcpTimeout : int) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout))
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, null)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionWhiteList : Id array) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, partitionWhiteList)
    new (brokerRouter : BrokerRouter) = new RoundRobinProducer(brokerRouter, CompressionCodec.None, null)

    /// Releases all connections and disposes the producer
    member __.Dispose() =
        (producer.Value :> IDisposable).Dispose()
        brokerRouter.Dispose()

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
        member self.Dispose() = self.Dispose()

/// Information about offsets
[<StructuredFormatDisplay("Id: {PartitionId}, Offset: {Offset}, Metadata: {Metadata}")>]
type PartitionOffset = { PartitionId : Id; Offset : Offset; Metadata : string }

/// Interface for offset managers
type IConsumerOffsetManager =
    inherit IDisposable
    /// Fetch offset for the specified topic and partitions
    abstract member Fetch : string -> PartitionOffset array
    /// Commit offset for the specified topic and partitions
    abstract member Commit : string * PartitionOffset seq -> unit

/// Offset manager for version 0. This commits and fetches offset to/from Zookeeper instances.
type ConsumerOffsetManagerV0(topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let refreshMetadataOnException f =
        try
            f()
        with
        | _ ->
            brokerRouter.RefreshMetadata()
            f()

    let innerFetch consumerGroup =
        let broker = brokerRouter.GetAllBrokers() |> Seq.head
        let partitions = brokerRouter.GetAvailablePartitionIds(topicName)
        let request = new OffsetFetchRequest(consumerGroup, [| { OffsetFetchRequestTopic.Name = topicName; Partitions = partitions } |], int16 0)
        let response = broker.Send(request)
        let offsets = response.Topics
                        |> Seq.filter (fun x -> x.Name = topicName)
                        |> Seq.map (fun x -> x.Partitions)
                        |> Seq.concat
                        |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
                        |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = x.Metadata; Offset = x.Offset })
                        |> Seq.toArray
        LogConfiguration.Logger.Info.Invoke(sprintf "Offsets fetched from Zookeeper, topic '%s', group '%s': %A" topicName consumerGroup offsets)
        offsets

    let handleOffsetCommitResponseCodes (offsetCommitResponse : OffsetCommitResponse) (offsets : seq<PartitionOffset>) (consumerGroup : string) (managerName : string) =
        let errorCodes = Seq.concat (offsetCommitResponse.Topics |> Seq.map (fun t -> t.Partitions |> Seq.filter (fun p -> p.ErrorCode <> ErrorCode.NoError) |> Seq.map(fun p -> sprintf "Topic: %s Partition: %i ErrorCode: %s" t.Name p.Id (p.ErrorCode.ToString()))))
        let topic = (offsetCommitResponse.Topics |> Seq.head).Name
        match Seq.isEmpty errorCodes with
        | false -> raiseWithErrorLog(ErrorCommittingOffsetException(managerName, topic, consumerGroup, errorCodes))
        | true -> LogConfiguration.Logger.Info.Invoke(sprintf "Offsets committed to %s, topic '%s', group '%s': %A" managerName topic consumerGroup offsets)

    let innerCommit offsets consumerGroup =
        let broker = brokerRouter.GetAllBrokers() |> Seq.head
        let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = x.Metadata; Offset = x.Offset }) |> Seq.toArray
        let request = new OffsetCommitV0Request(consumerGroup, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
        let response = broker.Send(request)
        handleOffsetCommitResponseCodes response offsets consumerGroup "Zookeeper"

    do
        brokerRouter.Connect()
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV0(topicName, new BrokerRouter(brokerSeeds, tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    /// Fetch offset for the specified topic and partitions
    member __.Fetch(consumerGroup) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerFetch consumerGroup)
    /// Commit offset for the specified topic and partitions
    member __.Commit(consumerGroup, offsets) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerCommit offsets consumerGroup)
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member self.Fetch(consumerGroup) = self.Fetch(consumerGroup)
        /// Commit offset for the specified topic and partitions
        member self.Commit(consumerGroup, offsets) = self.Commit(consumerGroup, offsets)
        member self.Dispose() = self.Dispose()

module internal ErrorHelper =
    let inline (|HasError|) errorCode (x : ^a seq) =
        x
        |> Seq.map (fun x -> (^a : (member ErrorCode : ErrorCode) (x)))
        |> Seq.contains errorCode


/// Offset manager for version 1. This commits and fetches offset to/from Kafka broker.
type ConsumerOffsetManagerV1(topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let coordinatorDictionary = new ConcurrentDictionary<string, Broker>()
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

    let rec innerFetch consumerGroup =
        let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
        let partitions = brokerRouter.GetAvailablePartitionIds(topicName)
        let request = new OffsetFetchRequest(consumerGroup, [| { OffsetFetchRequestTopic.Name = topicName; Partitions = partitions } |], int16 1)
        let response = coordinator.Send(request)
        let partitions =
            response.Topics
            |> Seq.filter (fun x -> x.Name = topicName)
            |> Seq.map (fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray
        match partitions with
        | ErrorHelper.HasError ErrorCode.ConsumerCoordinatorNotAvailable true | ErrorHelper.HasError ErrorCode.GroupLoadInProgressCode true -> innerFetch consumerGroup
        | ErrorHelper.HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerFetch consumerGroup
        | _ ->
            let offsets = partitions
                            |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
                            |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = ""; Offset = x.Offset })
                            |> Seq.toArray
            LogConfiguration.Logger.Info.Invoke(sprintf "Offsets fetched from Kafka, topic '%s', group '%s': %A" topicName consumerGroup offsets)
            offsets

    let handleOffsetCommitResponseCodes (offsetCommitResponse : OffsetCommitResponse) (offsets : seq<PartitionOffset>) (consumerGroup : string) (managerName : string) =
        let errorCodes = Seq.concat (offsetCommitResponse.Topics |> Seq.map (fun t -> t.Partitions |> Seq.filter (fun p -> p.ErrorCode <> ErrorCode.NoError) |> Seq.map(fun p -> sprintf "Topic: %s Partition: %i ErrorCode: %s" t.Name p.Id (p.ErrorCode.ToString()))))
        let topic = (offsetCommitResponse.Topics |> Seq.head).Name
        match Seq.isEmpty errorCodes with
        | false -> raiseWithErrorLog(ErrorCommittingOffsetException(managerName, topic, consumerGroup, errorCodes))
        | true -> LogConfiguration.Logger.Info.Invoke(sprintf "Offsets committed to %s, topic '%s', group '%s': %A" managerName topic consumerGroup offsets)

    let rec innerCommit consumerGroup offsets =
        let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
        let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV1Partition.Id = x.PartitionId; Metadata = ""; Offset = x.Offset; TimeStamp = DefaultTimestamp }) |> Seq.toArray
        let request = new OffsetCommitV1Request(consumerGroup, DefaultGenerationId, "", [| { OffsetCommitRequestV1Topic.Name = topicName; Partitions = partitions } |])
        let response = coordinator.Send(request)
        let partitions =
            response.Topics
            |> Seq.filter (fun x -> x.Name = topicName)
            |> Seq.map (fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray
        match partitions with
        | ErrorHelper.HasError ErrorCode.ConsumerCoordinatorNotAvailable true | ErrorHelper.HasError ErrorCode.GroupLoadInProgressCode true -> innerCommit consumerGroup offsets
        | ErrorHelper.HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerCommit consumerGroup offsets
        | _ ->
            handleOffsetCommitResponseCodes response offsets consumerGroup "Kafka"

    do
        brokerRouter.Connect()
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV1(topicName, new BrokerRouter(brokerSeeds, tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    /// Fetch offset for the specified topic and partitions
    member __.Fetch(consumerGroup) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerFetch consumerGroup)
    /// Commit offset for the specified topic and partitions
    member __.Commit(consumerGroup, offsets) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerCommit consumerGroup offsets)
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member self.Fetch(consumerGroup) = self.Fetch(consumerGroup)
        /// Commit offset for the specified topic and partitions
        member self.Commit(consumerGroup, offsets) = self.Commit(consumerGroup, offsets)
        member self.Dispose() = self.Dispose()

/// Offset manager for version 2. This commits and fetches offset to/from Kafka broker.
type ConsumerOffsetManagerV2(topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let coordinatorDictionary = new ConcurrentDictionary<string, Broker>()
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

    let rec innerFetch consumerGroup =
        let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
        let partitions = brokerRouter.GetAvailablePartitionIds(topicName)
        let request = new OffsetFetchRequest(consumerGroup, [| { OffsetFetchRequestTopic.Name = topicName; Partitions = partitions } |], int16 1)
        let response = coordinator.Send(request)
        let partitions =
            response.Topics
            |> Seq.filter (fun x -> x.Name = topicName)
            |> Seq.map (fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray
        match partitions with
        | ErrorHelper.HasError ErrorCode.ConsumerCoordinatorNotAvailable true | ErrorHelper.HasError ErrorCode.GroupLoadInProgressCode true -> innerFetch consumerGroup
        | ErrorHelper.HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerFetch consumerGroup
        | _ ->
            let offsets = partitions
                            |> Seq.filter (fun x -> x.ErrorCode.IsSuccess())
                            |> Seq.map (fun x -> { PartitionId = x.Id; Metadata = ""; Offset = x.Offset })
                            |> Seq.toArray
            LogConfiguration.Logger.Info.Invoke(sprintf "Offsets fetched from KafkaV2, topic '%s', group '%s': %A" topicName consumerGroup offsets)
            offsets

    let handleOffsetCommitResponseCodes (offsetCommitResponse : OffsetCommitResponse) (offsets : seq<PartitionOffset>) (consumerGroup : string) (managerName : string) =
        let errorCodes = Seq.concat (offsetCommitResponse.Topics |> Seq.map (fun t -> t.Partitions |> Seq.filter (fun p -> p.ErrorCode <> ErrorCode.NoError) |> Seq.map(fun p -> sprintf "Topic: %s Partition: %i ErrorCode: %s" t.Name p.Id (p.ErrorCode.ToString()))))
        let topic = (offsetCommitResponse.Topics |> Seq.head).Name
        match Seq.isEmpty errorCodes with
        | false -> raiseWithErrorLog(ErrorCommittingOffsetException(managerName, topic, consumerGroup, errorCodes))
        | true -> LogConfiguration.Logger.Info.Invoke(sprintf "Offsets committed to %s, topic '%s', group '%s': %A" managerName topic consumerGroup offsets)

    let rec innerCommit consumerGroup offsets =
        let coordinator = coordinatorDictionary.GetOrAdd(consumerGroup, getOffsetCoordinator)
        let partitions = offsets |> Seq.map (fun x -> { OffsetCommitRequestV0Partition.Id = x.PartitionId; Metadata = ""; Offset = x.Offset }) |> Seq.toArray
        let request = new OffsetCommitV2Request(consumerGroup, DefaultGenerationId, "", DefaultRetentionTime, [| { OffsetCommitRequestV0Topic.Name = topicName; Partitions = partitions } |])
        let response = coordinator.Send(request)
        let partitions =
            response.Topics
            |> Seq.filter (fun x -> x.Name = topicName)
            |> Seq.map (fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray
        match partitions with
        | ErrorHelper.HasError ErrorCode.ConsumerCoordinatorNotAvailable true | ErrorHelper.HasError ErrorCode.GroupLoadInProgressCode true -> innerCommit consumerGroup offsets
        | ErrorHelper.HasError ErrorCode.NotCoordinatorForConsumer true ->
            coordinatorDictionary.TryUpdate(consumerGroup, getOffsetCoordinator consumerGroup, coordinator) |> ignore
            innerCommit consumerGroup offsets
        | _ ->
            handleOffsetCommitResponseCodes response offsets consumerGroup "KafkaV2"

    do
        brokerRouter.Connect()
    new (brokerSeeds, topicName, tcpTimeout) = new ConsumerOffsetManagerV2(topicName, new BrokerRouter(brokerSeeds, tcpTimeout))
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true
    /// Fetch offset for the specified topic and partitions
    member __.Fetch(consumerGroup) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerFetch consumerGroup)
    /// Commit offset for the specified topic and partitions
    member __.Commit(consumerGroup, offsets) =
        raiseIfDisposed(disposed)

        refreshMetadataOnException (fun () -> innerCommit consumerGroup offsets)
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member self.Fetch(consumerGroup) = self.Fetch(consumerGroup)
        /// Commit offset for the specified topic and partitions
        member self.Commit(consumerGroup, offsets) = self.Commit(consumerGroup, offsets)
        member self.Dispose() = self.Dispose()

/// Offset manager commiting offfsets to both Zookeeper and Kafka, but only fetches from Zookeeper. Used when migrating from Zookeeper to Kafka.
type ConsumerOffsetManagerDualCommit(topicName, brokerRouter : BrokerRouter) =
    let mutable disposed = false
    let consumerOffsetManagerV0 = new ConsumerOffsetManagerV0(topicName, brokerRouter) :> IConsumerOffsetManager
    let consumerOffsetManagerV1 = new ConsumerOffsetManagerV1(topicName, brokerRouter) :> IConsumerOffsetManager
    new (brokerSeeds, topicName, tcpTimeout : int) = new ConsumerOffsetManagerDualCommit(topicName, new BrokerRouter(brokerSeeds, tcpTimeout))
    /// Fetch offset for the specified topic and partitions
    member __.Fetch(consumerGroup) =
        raiseIfDisposed(disposed)

        consumerOffsetManagerV0.Fetch(consumerGroup)
    /// Commit offset for the specified topic and partitions
    member __.Commit(consumerGroup, offsets) =
        raiseIfDisposed(disposed)

        consumerOffsetManagerV0.Commit(consumerGroup, offsets)
        consumerOffsetManagerV1.Commit(consumerGroup, offsets)
    member __.Dispose() =
        if not disposed then
            consumerOffsetManagerV0.Dispose()
            consumerOffsetManagerV1.Dispose()
            disposed <- true
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member self.Fetch(consumerGroup) = self.Fetch(consumerGroup)
        /// Commit offset for the specified topic and partitions
        member self.Commit(consumerGroup, offsets) = self.Commit(consumerGroup, offsets)
        member self.Dispose() = self.Dispose()

/// Noop offsetmanager, used when no offset should be commit
type DisabledConsumerOffsetManager() =
    interface IConsumerOffsetManager with
        /// Fetch offset for the specified topic and partitions
        member __.Fetch(_) = [||]
        /// Commit offset for the specified topic and partitions
        member __.Commit(_, _) = ()
        member __.Dispose() = ()

[<NoEquality;NoComparison>]
type MessageWithMetadata =
    {
        Offset : Offset;
        Message : Message;
        PartitionId : Id;
    }

type IConsumer =
    inherit IDisposable
    abstract member Consume : System.Threading.CancellationToken -> IEnumerable<MessageWithMetadata>
    abstract member GetPosition : unit -> PartitionOffset array
    abstract member SetPosition : PartitionOffset array -> unit
    abstract member OffsetManager : IConsumerOffsetManager

/// Offset storage type
type OffsetStorage =
    /// Do not use any offset storage
    | None = 0
    /// Store offsets on the Zookeeper
    | Zookeeper = 1
    /// Store offsets on the Kafka brokers, using version 1
    | Kafka = 2
    /// Store offsets both on the Zookeeper and Kafka brokers
    | DualCommit = 3
    /// Store offsets on the Kafka brokers, using version 2
    | KafkaV2 = 4

/// Consumer options
type ConsumerOptions() =
    let mutable partitionWhitelist = [||]
    let mutable offsetManager = OffsetStorage.Zookeeper
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
    member val OffsetStorage = offsetManager with get, set
    /// The number of milliseconds to wait before retrying, when the connection is lost during consuming. The default values is 5000.
    member val ConnectionRetryInterval = 5000 with get, set
    /// The partitions to consume messages from
    member __.PartitionWhitelist
        with get() = partitionWhitelist
        and set x =
            if x = null then
                partitionWhitelist <- [||]
            else
                partitionWhitelist <- x

[<AbstractClass>]
type BaseConsumer(topicName, brokerRouter : BrokerRouter, consumerOptions : ConsumerOptions) =
    let mutable disposed = false

    let offsetManager =
        match consumerOptions.OffsetStorage with
        | OffsetStorage.Zookeeper -> (new ConsumerOffsetManagerV0(topicName, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.Kafka -> (new ConsumerOffsetManagerV1(topicName, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.KafkaV2 -> (new ConsumerOffsetManagerV2(topicName, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.DualCommit -> (new ConsumerOffsetManagerDualCommit(topicName, brokerRouter)) :> IConsumerOffsetManager
        | _ -> (new DisabledConsumerOffsetManager()) :> IConsumerOffsetManager
    let partitionOffsets = new ConcurrentDictionary<Id, Offset>()
    let updateTopicPartitions (brokers : Broker seq) =
        brokers
        |> Seq.map (fun x -> x.LeaderFor)
        |> Seq.concat
        |> Seq.filter (fun x -> x.TopicName = topicName)
        |> Seq.map (fun x ->
            match consumerOptions.PartitionWhitelist with
            | [||] -> x.PartitionIds
            | _ -> Set.intersect (Set.ofArray x.PartitionIds) (Set.ofArray consumerOptions.PartitionWhitelist) |> Set.toArray)
        |> Seq.concat
        |> Seq.iter (fun id -> partitionOffsets.AddOrUpdate(id, new Func<Id, Offset>(fun _ -> int64 0), fun _ value -> value) |> ignore)

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

    let getThePartitionOffsetsWeWantToAddOrUpdate(offsets : PartitionOffset seq) =
        match (Seq.isEmpty consumerOptions.PartitionWhitelist) with
        | true -> offsets
        | false -> offsets |> Seq.filter (fun x -> consumerOptions.PartitionWhitelist |> Seq.exists (fun y -> y = x.PartitionId))
    
    do
        brokerRouter.Error.Add(fun x -> LogConfiguration.Logger.Fatal.Invoke(sprintf "Unhandled exception in BrokerRouter", x))
        brokerRouter.Connect()
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)

    /// The position of the consumer
    abstract member PartitionOffsets : ConcurrentDictionary<Id, Offset> with get
    /// Consume messages from the topic specified in the consumer. This function returns a sequence of messages, the size is defined by the chunk size.
    /// Multiple calls to this method consumes the next chunk of messages.
    abstract member ConsumeInChunks : Id * int option -> Async<seq<MessageWithMetadata>>
    /// Get the current consumer offsets
    abstract member GetPosition : unit -> PartitionOffset array
    /// Sets the current consumer offsets
    abstract member SetPosition : PartitionOffset seq -> unit
    /// Gets the offset manager
    abstract member OffsetManager : IConsumerOffsetManager

    /// Gets the offset manager
    default __.OffsetManager = offsetManager

    /// The position of the consumer
    default __.PartitionOffsets with get() = partitionOffsets

    /// Get the current consumer offsets
    default __.GetPosition() =
        partitionOffsets |> Seq.map (fun x -> { PartitionId = x.Key; Offset = x.Value; Metadata = String.Empty }) |> Seq.toArray

    /// Sets the current consumer offsets
    default __.SetPosition(offsets : PartitionOffset seq) =
        offsets
        |> getThePartitionOffsetsWeWantToAddOrUpdate
        |> Seq.iter (fun x -> partitionOffsets.AddOrUpdate(x.PartitionId, new Func<Id, Offset>(fun _ -> x.Offset), fun _ _ -> x.Offset) |> ignore)

    /// Consume messages from the topic specified in the consumer. This function returns a sequence of messages, the size is defined by the chunk size.
    /// Multiple calls to this method consumes the next chunk of messages.
    default self.ConsumeInChunks(partitionId, maxBytes : int option) =
        async {
            try
                let (_, offset) = partitionOffsets.TryGetValue(partitionId)
                let request = new FetchRequest(-1, consumerOptions.MaxWaitTime, consumerOptions.MinBytes, [| { Name = topicName; Partitions = [| { FetchOffset = offset; Id = partitionId; MaxBytes = defaultArg maxBytes consumerOptions.MaxBytes } |] } |])
                let response = brokerRouter.TrySendToBroker(topicName, partitionId, request)
                let partitionResponse = response.Topics |> Seq.map (fun x -> x.Partitions) |> Seq.concat |> Seq.head
                match partitionResponse.ErrorCode with
                | ErrorCode.NoError | ErrorCode.ReplicaNotAvailable ->
                    let messages =
                        partitionResponse.MessageSets
                        |> Compression.DecompressMessageSets
                        |> Seq.map (fun x -> { Message = x.Message; Offset = x.Offset; PartitionId = partitionId })
                    if partitionResponse.MessageSets |> Seq.isEmpty |> not then
                        let nextOffset = (partitionResponse.MessageSets |> Seq.map (fun x -> x.Offset) |> Seq.max) + int64 1
                        partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> nextOffset), fun _ _ -> nextOffset) |> ignore
                    return messages
                | ErrorCode.NotLeaderForPartition ->
                    brokerRouter.RefreshMetadata()
                    return! self.ConsumeInChunks(partitionId, maxBytes)
                | ErrorCode.OffsetOutOfRange ->
                    let broker = brokerRouter.GetBroker(topicName, partitionId)
                    let earliestOffset = handleOffsetOutOfRangeError broker partitionId topicName
                    partitionOffsets.AddOrUpdate(partitionId, new Func<Id, Offset>(fun _ -> earliestOffset), fun _ _ -> earliestOffset) |> ignore
                    return! self.ConsumeInChunks(partitionId, maxBytes)
                | _ ->
                    raise(BrokerReturnedErrorException partitionResponse.ErrorCode)
                    return Seq.empty<_>
            with
            | :? BufferOverflowException ->
                let increasedFetchSize = (defaultArg maxBytes consumerOptions.MaxBytes) * 2
                LogConfiguration.Logger.Info.Invoke(sprintf "Temporarily increasing fetch size to %i to accommodate increased message size." increasedFetchSize)
                return! self.ConsumeInChunks(partitionId, Some increasedFetchSize)
            | e ->
                LogConfiguration.Logger.Error.Invoke(sprintf "Got exception while consuming from topic '%s' partition '%i'. Retrying in %i milliseconds" topicName partitionId consumerOptions.ConnectionRetryInterval, e)
                do! Async.Sleep consumerOptions.ConnectionRetryInterval
                return Seq.empty<_>
        }

    member __.Dispose() =
        if not disposed then
            offsetManager.Dispose()
            brokerRouter.Dispose()
            disposed <- true

    member __.CheckDisposedState() =
        raiseIfDisposed(disposed)

    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// High level kafka consumer.
type Consumer(topicName, consumerOptions : ConsumerOptions, brokerRouter : BrokerRouter) =
    inherit BaseConsumer(topicName, brokerRouter, consumerOptions)

    new (brokerSeeds, topicName, consumerOptions : ConsumerOptions) = new Consumer(topicName, consumerOptions, new BrokerRouter(brokerSeeds, consumerOptions.TcpTimeout))
    new (brokerSeeds, topicName) = new Consumer(brokerSeeds, topicName, new ConsumerOptions())
    /// Consume messages from the topic specified in the consumer. This function returns a blocking IEnumerable. Also returns offset of the message.
    member self.Consume(cancellationToken : System.Threading.CancellationToken) =
        base.CheckDisposedState()
        let blockingCollection = new System.Collections.Concurrent.BlockingCollection<_>()
        let rec consume() =
            async {
                let! messagesFromAllPartitions =
                    self.PartitionOffsets.Keys
                    |> Seq.map (fun x -> async { return! self.ConsumeInChunks(x, None) })
                    |> Async.Parallel
                messagesFromAllPartitions
                |> Seq.concat
                |> Seq.iter (fun x -> blockingCollection.Add(x))
                return! consume()
            }
        Async.Start(consume(), cancellationToken)
        blockingCollection.GetConsumingEnumerable(cancellationToken)
    /// Releases all connections and disposes the consumer
    member __.Dispose() =
        base.Dispose()
    interface IConsumer with
        member self.GetPosition() =
            self.GetPosition()
        member self.SetPosition(offsets) =
            self.SetPosition(offsets)
        member self.OffsetManager = self.OffsetManager
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.Dispose() = self.Dispose()

/// High level kafka consumer, consuming messages in chunks defined by MaxBytes, MinBytes and MaxWaitTime in the consumer options. Each call to the consume functions,
/// will provide a new chunk of messages. If no messages are available an empty sequence will be returned.
type ChunkedConsumer(topicName, consumerOptions : ConsumerOptions, brokerRouter : BrokerRouter) =
    inherit BaseConsumer(topicName, brokerRouter, consumerOptions)

    new (brokerSeeds, topicName, consumerOptions : ConsumerOptions) = new ChunkedConsumer(topicName, consumerOptions, new BrokerRouter(brokerSeeds, consumerOptions.TcpTimeout))
    new (brokerSeeds, topicName) = new ChunkedConsumer(brokerSeeds, topicName, new ConsumerOptions())
    /// Consume messages from the topic specified in the consumer. This function returns a sequence of messages, the size is defined by the chunk size.
    /// Multiple calls to this method consumes the next chunk of messages.
    member self.Consume(_) =
        base.CheckDisposedState()
        self.PartitionOffsets.Keys
        |> Seq.map (fun x -> async { return! self.ConsumeInChunks(x, None) })
        |> Async.Parallel
        |> Async.RunSynchronously
        |> Seq.concat
    /// Releases all connections and disposes the consumer
    member __.Dispose() =
        base.Dispose()
    interface IConsumer with
        member self.GetPosition() =
            self.GetPosition()
        member self.SetPosition(offsets) =
            self.SetPosition(offsets)
        member self.OffsetManager = self.OffsetManager
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.Dispose() = self.Dispose()
