namespace Franz.HighLevel

#nowarn "40"

open System
open System.Collections.Generic
open Franz
open System.Collections.Concurrent
open Franz.Compression
open Franz.Internal
open System.Threading        

type ConsumerException (msg, innerException : exn) =
    inherit Exception(msg, innerException)

type ErrorCommittingOffsetException (offsetManagerName : string, topic : string, consumerGroup : string, errorCodes : seq<string>) =
    inherit Exception()
    member __.Codes = errorCodes
    member __.OffsetManagerName = offsetManagerName
    member __.Topic = topic
    member __.ConsumerGroup = consumerGroup
    override e.Message = sprintf "One or more errors occoured while committing offsets (%s) for topic '%s' group '%s': %A" e.OffsetManagerName e.Topic e.ConsumerGroup e.Codes

type RequestTimedOutException() =
    inherit Exception()
    override __.Message = "Producer received RequestTimedOut on Ack from Brokers"

type RequestTimedOutRetryExceededException() =
    inherit Exception()
    override __.Message = "Producer received RequestTimedOut on Ack from Brokers to many times"

type BrokerReturnedErrorException (errorCode : ErrorCode) =
    inherit Exception()
    member __.Code = errorCode
    override e.Message = sprintf "Broker returned a response with error code: %s" (e.Code.ToString())

module Seq =
    /// Helper function to convert a sequence to a List<T>
    let toBclList (x : 'a seq) =
        new List<_>(x)

type PartiontionIds = List<Id>
type NextPartitionId = Id
type TopicPartitions = Dictionary<string, (PartiontionIds * NextPartitionId)>

type Message = { Value : string; Key : string }

type IProducer =
    inherit IDisposable
    abstract member SendMessage : string * Message * RequiredAcks * int -> unit
    abstract member SendMessage : string * Message -> unit
    abstract member SendMessages : string * Message array -> unit
    abstract member SendMessages : string * Message array * RequiredAcks * int -> unit

[<AbstractClass>]
type BaseProducer (brokerRouter : IBrokerRouter, compressionCodec, partitionSelector : Func<string, string, Id>) =
    let mutable disposed = false

    let retryOnRequestTimedOut retrySendFunction (retryCount : int) =
        LogConfiguration.Logger.Warning.Invoke(sprintf "Producer received RequestTimedOut on Ack from Brokers, retrying (%i) with increased timeout" retryCount, RequestTimedOutException())
        if retryCount > 1 then raiseWithErrorLog(RequestTimedOutRetryExceededException())
        retrySendFunction()
    
    let rec send key topicName messages requiredAcks brokerProcessingTimeout retryCount =
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
            send key topicName messages requiredAcks brokerProcessingTimeout 0
        | ErrorCode.RequestTimedOut ->
            retryOnRequestTimedOut (fun () -> send key topicName messages requiredAcks (brokerProcessingTimeout * 2) (retryCount + 1)) retryCount
        | _ -> raiseWithErrorLog(BrokerReturnedErrorException partitionResponse.ErrorCode)

    let sendMessages key topic requiredAcks brokerProcessingTimeout messages =
        let messageSets =
            messages
            |> Seq.map (fun x -> MessageSet.Create(int64 -1, int8 0, System.Text.Encoding.UTF8.GetBytes(if x.Key <> null then x.Key else ""), System.Text.Encoding.UTF8.GetBytes(x.Value)))
            |> Seq.toArray
        try
            let key = if key <> null then key else ""
            send key topic messageSets requiredAcks brokerProcessingTimeout 0
        with
        | _ ->
            brokerRouter.RefreshMetadata()
            send key topic messageSets requiredAcks brokerProcessingTimeout 0

    do
        brokerRouter.Error.Add(fun x -> LogConfiguration.Logger.Fatal.Invoke(sprintf "Unhandled exception in BrokerRouter", x))
        brokerRouter.Connect()

    /// Sends a message to the specified topic
    abstract member SendMessages : string * Message array * RequiredAcks * int -> unit

    /// Sends a message to the specified topic
    default __.SendMessages(topic, messages, requiredAcks, brokerProcessingTimeout) =
        let messagesGroupedByKey = messages |> Seq.groupBy (fun x -> x.Key)
        messagesGroupedByKey |> Seq.iter (fun (key, msgs) -> msgs |> sendMessages key topic requiredAcks brokerProcessingTimeout)

    /// Get all available brokers
    member __.GetAllBrokers() = brokerRouter.GetAllBrokers()

    /// Releases all connections and disposes the producer
    member __.Dispose() =
        if not disposed then
            brokerRouter.Dispose()
            disposed <- true

    /// Releases all connections and disposes the producer
    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// High level kafka producer
type Producer(brokerRouter : IBrokerRouter, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) =
    inherit BaseProducer(brokerRouter, compressionCodec, partitionSelector)

    new (brokerSeeds, partitionSelector : Func<string, string, Id>) = new Producer(brokerSeeds, 10000, partitionSelector)
    new (brokerSeeds, tcpTimeout : int, partitionSelector : Func<string, string, Id>) = new Producer(new BrokerRouter(brokerSeeds, tcpTimeout), partitionSelector)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionSelector : Func<string, string, Id>) = new Producer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, partitionSelector)
    new (brokerRouter : IBrokerRouter, partitionSelector : Func<string, string, Id>) = new Producer(brokerRouter, CompressionCodec.None, partitionSelector)
    
    /// Sends a message to the specified topic
    member self.SendMessages(topicName : string, message : Message array) =
        self.SendMessages(topicName, message, RequiredAcks.LocalLog, 500)
    
    /// Sends a message to the specified topic
    member self.SendMessage(topicName : string, message : Message) =
        self.SendMessages(topicName, [| message |])

    /// Sends a message to the specified topic
    member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
        self.SendMessages(topicName, [| message |], requiredAcks, brokerProcessingTimeout)
    
    /// Releases all connections and disposes the producer
    member self.Dispose() = (self :> IDisposable).Dispose()
    
    interface IProducer with
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, message) =
            self.SendMessages(topicName, [| message |])
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, messages) = 
            self.SendMessages(topicName, messages)
        member self.Dispose() = self.Dispose()

/// Producer sending messages in a round-robin fashion
type RoundRobinProducer(brokerRouter : IBrokerRouter, compressionCodec : CompressionCodec, partitionWhiteList : Id array) =
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
        producer <- Some <| new Producer(brokerRouter, compressionCodec, new Func<string, string, Id>(getNextPartitionId))
        brokerRouter.GetAllBrokers() |> updateTopicPartitions
        brokerRouter.MetadataRefreshed.Add(fun x -> x |> updateTopicPartitions)

    new (brokerSeeds) = new RoundRobinProducer(brokerSeeds, 10000)
    new (brokerSeeds, tcpTimeout : int) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout))
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, null)
    new (brokerSeeds, tcpTimeout : int, compressionCodec : CompressionCodec, partitionWhiteList : Id array) = new RoundRobinProducer(new BrokerRouter(brokerSeeds, tcpTimeout), compressionCodec, partitionWhiteList)
    new (brokerRouter : IBrokerRouter) = new RoundRobinProducer(brokerRouter, CompressionCodec.None, null)

    /// Releases all connections and disposes the producer
    member __.Dispose() =
        (producer.Value :> IDisposable).Dispose()

    /// Sends a message to the specified topic
    member __.SendMessages(topicName, message) =
        producer.Value.SendMessages(topicName, message, RequiredAcks.LocalLog, 500)
    
    /// Sends a message to the specified topic
    member __.SendMessages(topicName, messages : Message array, requiredAcks, brokerProcessingTimeout) =
        producer.Value.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout)
    
    /// Sends a message to the specified topic
    member self.SendMessage(topicName : string, message : Message) =
        self.SendMessages(topicName, [| message |])

    interface IProducer with
        member self.SendMessage(topicName, message, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, [| message |], requiredAcks, brokerProcessingTimeout)
        member self.SendMessage(topicName, message) =
            self.SendMessages(topicName, [| message |])
        member self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout) =
            self.SendMessages(topicName, messages, requiredAcks, brokerProcessingTimeout)
        member self.SendMessages(topicName, messages) =
            self.SendMessages(topicName, messages)
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
type ConsumerOffsetManagerV0(topicName, brokerRouter : IBrokerRouter) =
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
        let topic =
            match offsetCommitResponse.Topics |> Seq.tryHead with
            | Some x -> x.Name
            | None -> "Could not get topic name"
        match Seq.isEmpty errorCodes with
        | false -> raiseWithErrorLog(ErrorCommittingOffsetException(managerName, topic, consumerGroup, errorCodes))
        | true -> LogConfiguration.Logger.Info.Invoke(sprintf "Offsets committed to %s, topic '%s', group '%s': %A" managerName topic consumerGroup offsets)

    let innerCommit offsets consumerGroup =
        if offsets |> Seq.isEmpty then ()
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
type ConsumerOffsetManagerV1(topicName, brokerRouter : IBrokerRouter) =
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
        allBrokers |> Seq.filter (fun x -> x.Id = response.CoordinatorId) |> Seq.exactlyOne
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
type ConsumerOffsetManagerV2(topicName, brokerRouter : IBrokerRouter) =
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
        allBrokers |> Seq.filter (fun x -> x.Id = response.CoordinatorId) |> Seq.exactlyOne
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
type ConsumerOffsetManagerDualCommit(topicName, brokerRouter : IBrokerRouter) =
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

/// A message with offset and partition id
[<NoEquality;NoComparison>]
type MessageWithMetadata =
    {
        /// The offset of the message
        Offset : Offset;
        /// The message
        Message : Messages.Message;
        /// The partition id
        PartitionId : Id;
    }

type IConsumer =
    inherit IDisposable
    /// Consume messages
    abstract member Consume : System.Threading.CancellationToken -> IEnumerable<MessageWithMetadata>
    /// Get the current consumer position
    abstract member GetPosition : unit -> PartitionOffset array
    /// Set the current consumer position
    abstract member SetPosition : PartitionOffset seq -> unit
    /// Gets the offset manager
    abstract member OffsetManager : IConsumerOffsetManager
    /// Gets the broker router
    abstract member BrokerRouter : IBrokerRouter

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
    abstract member TcpTimeout : int with get, set
    
    /// The timeout for sending and receiving TCP data in milliseconds. Default value is 10000.
    default val TcpTimeout = 10000 with get, set
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
    /// Gets or sets the consumer topic
    member val Topic = "" with get, set

/// Base class for consumers, containing shared functionality
[<AbstractClass>]
type BaseConsumer(brokerRouter : IBrokerRouter, consumerOptions : ConsumerOptions) =
    let mutable disposed = false

    let offsetManager =
        match consumerOptions.OffsetStorage with
        | OffsetStorage.Zookeeper -> (new ConsumerOffsetManagerV0(consumerOptions.Topic, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.Kafka -> (new ConsumerOffsetManagerV1(consumerOptions.Topic, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.KafkaV2 -> (new ConsumerOffsetManagerV2(consumerOptions.Topic, brokerRouter)) :> IConsumerOffsetManager
        | OffsetStorage.DualCommit -> (new ConsumerOffsetManagerDualCommit(consumerOptions.Topic, brokerRouter)) :> IConsumerOffsetManager
        | _ -> (new DisabledConsumerOffsetManager()) :> IConsumerOffsetManager
    let partitionOffsets = new ConcurrentDictionary<Id, Offset>()
    let updateTopicPartitions (brokers : Broker seq) =
        brokers
        |> Seq.map (fun x -> x.LeaderFor)
        |> Seq.concat
        |> Seq.filter (fun x -> x.TopicName = consumerOptions.Topic)
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

    /// Gets the broker router
    member __.BrokerRouter = brokerRouter

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
                let request = new FetchRequest(-1, consumerOptions.MaxWaitTime, consumerOptions.MinBytes, [| { Name = consumerOptions.Topic; Partitions = [| { FetchOffset = offset; Id = partitionId; MaxBytes = defaultArg maxBytes consumerOptions.MaxBytes } |] } |])
                let response = brokerRouter.TrySendToBroker(consumerOptions.Topic, partitionId, request)
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
                    let broker = brokerRouter.GetBroker(consumerOptions.Topic, partitionId)
                    let earliestOffset = handleOffsetOutOfRangeError broker partitionId consumerOptions.Topic
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
                LogConfiguration.Logger.Error.Invoke(sprintf "Got exception while consuming from topic '%s' partition '%i'. Retrying in %i milliseconds" consumerOptions.Topic partitionId consumerOptions.ConnectionRetryInterval, e)
                do! Async.Sleep consumerOptions.ConnectionRetryInterval
                return Seq.empty<_>
        }

    /// Dispose the consumer
    member __.Dispose() =
        if not disposed then
            offsetManager.Dispose()
            brokerRouter.Dispose()
            disposed <- true

    member internal __.CheckDisposedState() =
        raiseIfDisposed(disposed)

    member internal __.ClearPositions() =
        partitionOffsets.Clear()

    interface IDisposable with
        member self.Dispose() = self.Dispose()

/// High level kafka consumer.
type Consumer(brokerRouter : IBrokerRouter, consumerOptions : ConsumerOptions) =
    inherit BaseConsumer(brokerRouter, consumerOptions)

    new (brokerSeeds, consumerOptions : ConsumerOptions) = new Consumer(new BrokerRouter(brokerSeeds, consumerOptions.TcpTimeout), consumerOptions)
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

    interface IConsumer with
        /// Get the current consumer position
        member self.GetPosition() =
            self.GetPosition()
        /// Set the current consumer position
        member self.SetPosition(offsets) =
            self.SetPosition(offsets)
        member self.OffsetManager = self.OffsetManager
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.Dispose() = self.Dispose()
        /// Gets the broker router
        member self.BrokerRouter = self.BrokerRouter

/// High level kafka consumer, consuming messages in chunks defined by MaxBytes, MinBytes and MaxWaitTime in the consumer options. Each call to the consume functions,
/// will provide a new chunk of messages. If no messages are available an empty sequence will be returned.
type ChunkedConsumer(brokerRouter : IBrokerRouter, consumerOptions : ConsumerOptions) =
    inherit BaseConsumer(brokerRouter, consumerOptions)

    new (brokerSeeds, consumerOptions : ConsumerOptions) = new ChunkedConsumer(new BrokerRouter(brokerSeeds, consumerOptions.TcpTimeout), consumerOptions)
    /// Consume messages from the topic specified in the consumer. This function returns a sequence of messages, the size is defined by the chunk size.
    /// Multiple calls to this method consumes the next chunk of messages.
    member self.Consume(cancellationToken : System.Threading.CancellationToken) =
        base.CheckDisposedState()
        let consume =
            self.PartitionOffsets.Keys
            |> Seq.map (fun x -> async { return! self.ConsumeInChunks(x, None) })
            |> Async.Parallel
        Async.RunSynchronously(consume, cancellationToken = cancellationToken)
        |> Seq.concat
    
    /// Releases all connections and disposes the consumer
    member __.Dispose() =
        base.Dispose()
    
    interface IConsumer with
        /// Get the current consumer position
        member self.GetPosition() =
            self.GetPosition()
        /// Set the current consumer position
        member self.SetPosition(offsets) =
            self.SetPosition(offsets)
        member self.OffsetManager = self.OffsetManager
        member self.Consume(cancellationToken) =
            self.Consume(cancellationToken)
        member self.Dispose() = self.Dispose()
        /// Gets the broker router
        member self.BrokerRouter = self.BrokerRouter

/// Interface used to implement consumer group assignors
type IAssignor =
    /// Name of the assignor
    abstract member Name : string
    /// Perform group assignment given group members and available partition ids
    abstract member Assign : GroupMember seq * Id seq -> GroupAssignment array

/// Assigns partitions to group memebers using round robin
type RoundRobinAssignor(topic) =
    let version = 0s

    /// Name of the assignor
    member __.Name = "roundrobin"
    /// Assign partitions to members
    member __.Assign(members : GroupMember seq, partitions : Id seq) =
        let rec memberSeq = seq {
            for x in members do yield x
            yield! memberSeq
        }
        let assignments = new Dictionary<string, int list>()
        members |> Seq.iter (fun x -> assignments.Add(x.MemberId, []))
        partitions
        |> Seq.iter2 (fun (m : GroupMember) p -> assignments.[m.MemberId] <- p :: assignments.[m.MemberId]) memberSeq
        assignments
        |> Seq.map (fun x -> { GroupAssignment.MemberId = x.Key; MemberAssignment = { Version = version; UserData = [||]; PartitionAssignment = [| { Topic = topic; Partitions = x.Value |> Seq.toArray } |] } })
        |> Seq.toArray

    interface IAssignor with
        /// Name of the assignor
        member self.Name = self.Name
        /// Perform group assignment given group members and available partition ids
        member self.Assign(members, partitions) = self.Assign(members, partitions)

type internal CoordinatorMessage =
    | JoinGroup of CancellationToken
    | LeaveGroup

type GroupConsumerOptions() =
    inherit ConsumerOptions()
    /// Gets or sets the interval between heartbeats. The default values is 3000 ms.
    member val HeartbeatInterval = 3000 with get, set
    /// Gets or sets the session timeout. The default value is 30000 ms.
    member val SessionTimeout = 30000 with get, set
    /// Gets or sets the TCP timeout, this value must be greater than the session timeout. The default value is 40000 ms.
    override val TcpTimeout = 40000 with get, set
    /// Gets or sets the available assignors
    member val Assignors = Seq.empty with get, set
    /// Gets or sets the group id
    member val GroupId = "" with get, set

/// High level kafka consumer using the group management features of Kafka.
type GroupConsumer(brokerRouter : BrokerRouter, options : GroupConsumerOptions) =
    let mutable disposed = false
    let groupCts = new CancellationTokenSource()
    let innerMessageQueue = new ConcurrentQueue<MessageWithMetadata>()
    let queueEmptyEvent = new ManualResetEventSlim(true)
    let queueAvailableEvent = new AutoResetEvent(false)
    let mutable fatalException : Exception option = None
    let messageQueue =
        let rec loop() =
            let success, msg = innerMessageQueue.TryDequeue()
            if groupCts.IsCancellationRequested then Seq.empty
            else
                if success then seq { yield msg; yield! loop() }
                else
                    queueEmptyEvent.Set()
                    waitForData()
        and waitForData() =
            let gotData = queueAvailableEvent.WaitOne(100)
            if fatalException.IsSome then
                LogConfiguration.Logger.Fatal.Invoke(fatalException.Value.Message, fatalException.Value)
                raise (ConsumerException(fatalException.Value.Message, fatalException.Value))
            if gotData then
                queueEmptyEvent.Reset()
                seq { yield! loop() }
            else
                if groupCts.IsCancellationRequested then Seq.empty
                else waitForData()
            
        seq { yield! loop() }

    let setFatalException e =
        fatalException <- Some (e :> Exception)
                
    let consumer =
        if options.TcpTimeout < options.HeartbeatInterval then invalidOp "TCP timeout must be greater than heartbeat interval"
        if options.GroupId |> String.IsNullOrEmpty then invalidOp "Group id cannot be null or empty"
        new ChunkedConsumer(brokerRouter, options)

    let rec getGroupCoordinatorId() =
        let response =
            new ConsumerMetadataRequest(options.GroupId)
            |> consumer.BrokerRouter.TrySendToBroker
        if response.CoordinatorId = -1 then getGroupCoordinatorId()
        else response.CoordinatorId

    let tryFindAssignor protocol : IAssignor option =
        options.Assignors |> Seq.tryFind (fun x -> x.Name = protocol)

    let getAssigment response =
        match tryFindAssignor response.GroupProtocol with
        | Some x ->
            consumer.BrokerRouter.RefreshMetadata()
            x.Assign(response.Members, consumer.BrokerRouter.GetAvailablePartitionIds(options.Topic))
        | None -> raiseWithErrorLog (invalidOp (sprintf "Coordinator selected unsupported protocol %s" response.GroupProtocol))

    let trySendToBroker coordinatorId request = consumer.BrokerRouter.TrySendToBroker(coordinatorId, request)
    let createSyncRequest response assignment = new SyncGroupRequest(options.GroupId, response.GenerationId, response.MemberId, assignment)
    
    let joinAsLeader response =
        response
        |> getAssigment
        |> createSyncRequest response
    
    let joinAsFollower response = createSyncRequest response [||]
    
    let (|Leader|Follower|) response =
        if response.LeaderId = response.MemberId then Leader
        else Follower
        
    let sendJoinGroupAsync (coordinatorId : Id) success failure =
        async {
            try
                let response =
                    new JoinGroupRequest(options.GroupId, options.SessionTimeout, "", "consumer", options.Assignors |> Seq.map (fun x -> { Name = x.Name; Metadata = [||] }))
                    |> trySendToBroker coordinatorId
                match response.ErrorCode with
                | ErrorCode.NoError -> return! success response
                | _ -> return! failure response.ErrorCode
            with
                e ->
                    LogConfiguration.Logger.Warning.Invoke("Got unexpected exception while joining group. Trying to rejoin...", e)
                    return! failure ErrorCode.Unknown
        }

    let sendSyncGroupRequestAsync (request : SyncGroupRequest) (coordinatorId : Id) success failure =
        async {
            try
                let response = request |> trySendToBroker coordinatorId
                match response.ErrorCode with
                | ErrorCode.NoError -> return! success coordinatorId response
                | _ -> return! failure response.ErrorCode
            with
                e ->
                    LogConfiguration.Logger.Warning.Invoke("Got unexpected exception while syncing group. Trying to rejoin...", e)
                    return! failure ErrorCode.Unknown
        }

    let sendHeartbeatRequestAsync (generationId : Id) memberId (coordinatorId : Id) cts success failure =
        async {
            try
                let response =
                    new HeartbeatRequest(options.GroupId, generationId, memberId)
                    |> trySendToBroker coordinatorId
                match response.ErrorCode with
                | ErrorCode.NoError -> return! success cts generationId memberId coordinatorId
                | _ -> return! failure cts response.ErrorCode
            with
                e ->
                    LogConfiguration.Logger.Warning.Invoke("Got unexpected exception while sending heartbeat. Trying to rejoin...", e)
                    return! failure cts ErrorCode.Unknown
        }

    let updateConsumerPosition (partitionAssignments : PartitionAssignment array) =
        consumer.ClearPositions()
        let assignedPartitions =
            partitionAssignments
            |> Seq.filter (fun x -> x.Topic = options.Topic)
            |> Seq.map(fun x -> x.Partitions)
            |> Seq.concat
            |> Seq.toArray

        let offsetsForAssignedPartitions =
            consumer.OffsetManager.Fetch(options.GroupId)
            |> Seq.filter (fun x -> assignedPartitions |> Seq.contains(x.PartitionId))

        let unavailableOffsets =
            assignedPartitions
            |> Seq.filter (fun x -> offsetsForAssignedPartitions |> Seq.map(fun y -> y.PartitionId) |> Seq.contains(x) |> not)
            |> Seq.map (fun x -> { PartitionOffset.Offset = 0L; PartitionOffset.PartitionId = x; PartitionOffset.Metadata = "" })

        let offsetsForAssignedPartitions =
            unavailableOffsets
            |> Seq.append offsetsForAssignedPartitions

        consumer.SetPosition(offsetsForAssignedPartitions)

    let joinGroup success failure =
        let coordinatorId = getGroupCoordinatorId()
        sendJoinGroupAsync coordinatorId (success coordinatorId) failure

    let consumeAsync(token) =
        async {
            while true do
                if queueEmptyEvent.IsSet then
                    try
                        let msgs = consumer.Consume(token)
                        if msgs |> Seq.isEmpty |> not then
                            msgs
                            |> Seq.iter innerMessageQueue.Enqueue
                            queueAvailableEvent.Set() |> ignore
                    with
                        :? OperationCanceledException -> return ()
        }

    let stopConsuming (cts : CancellationTokenSource) =
        LogConfiguration.Logger.Trace.Invoke("Stopping consuming...")
        cts.Cancel()

    let createLinkedCancellationTokenSource token =
        CancellationTokenSource.CreateLinkedTokenSource([| token; groupCts.Token |])

    let agent = Agent<CoordinatorMessage>.Start(fun inbox ->
            let mutable agentCts : CancellationTokenSource = null
            let rec notConnectedState () =
                async {
                    let! msg = inbox.Receive()
                    match msg with
                    | JoinGroup token ->
                        agentCts <- createLinkedCancellationTokenSource token
                        return! joinGroup joiningState failedJoinState
                    | LeaveGroup -> return! notConnectedState()
                }
            and reconnectState () =
                async {
                    let currentPosition = consumer.GetPosition()
                    if currentPosition |> Seq.isEmpty |> not then
                        try
                            consumer.OffsetManager.Commit(options.GroupId, currentPosition)
                        with e -> LogConfiguration.Logger.Error.Invoke("Could not save offsets before rejoining group", e)
                    LogConfiguration.Logger.Trace.Invoke(sprintf "Rejoining group '%s' in %i ms" options.GroupId options.ConnectionRetryInterval)
                    let! msg = inbox.TryReceive(options.ConnectionRetryInterval)
                    match msg with
                    | Some x ->
                        match x with
                        | LeaveGroup -> return! notConnectedState()
                        | JoinGroup token ->
                            agentCts <- createLinkedCancellationTokenSource token
                            return! joinGroup joiningState failedJoinState
                    | None ->
                        if agentCts.IsCancellationRequested then ()
                        else
                            return! joinGroup joiningState failedJoinState
                }
            and failedJoinState (errorCode : ErrorCode) =
                async {
                    match errorCode with
                    | ErrorCode.ConsumerCoordinatorNotAvailable
                    | ErrorCode.GroupLoadInProgressCode
                    | ErrorCode.NotCoordinatorForConsumer
                    | ErrorCode.UnknownMemberIdCode ->
                        LogConfiguration.Logger.Info.Invoke(sprintf "Joining group '%s' failed with %O. Trying to rejoin..." options.GroupId errorCode)
                        return! reconnectState()
                    | ErrorCode.InconsistentGroupProtocolCode ->
                        setFatalException (InvalidOperationException(sprintf "Could not join group '%s', as none for the protocols requested are supported" options.GroupId))
                    | ErrorCode.InvalidSessionTimeoutCode ->
                        setFatalException (InvalidOperationException(sprintf "Tried to join group '%s' with invalid session timeout" options.GroupId))
                    | ErrorCode.GroupAuthorizationFailedCode ->
                        setFatalException (InvalidOperationException(sprintf "Not authorized to join group '%s'" options.GroupId))
                    | _ ->
                        let ex = InvalidOperationException(sprintf "Got unexpected error code, while trying to join group '%s'. Trying to rejoin..." options.GroupId)
                        LogConfiguration.Logger.Error.Invoke(ex.Message, ex)
                        return! reconnectState()
                }
            and joiningState (coordinatorId : Id) (response : JoinGroupResponse) =
                async {
                    LogConfiguration.Logger.Trace.Invoke(sprintf "Syncing consumer group '%s'" options.GroupId)
                    let syncRequest =
                        response
                        |> match response with
                            | Leader -> joinAsLeader
                            | Follower -> joinAsFollower
                    return! sendSyncGroupRequestAsync syncRequest coordinatorId (syncingState response.GenerationId response.MemberId) failedSyncState
                }
            and failedSyncState (errorCode : ErrorCode) =
                async {
                    match errorCode with
                    | ErrorCode.ConsumerCoordinatorNotAvailable
                    | ErrorCode.NotCoordinatorForConsumer
                    | ErrorCode.IllegalGenerationCode
                    | ErrorCode.UnknownMemberIdCode
                    | ErrorCode.RebalanceInProgressCode ->
                        LogConfiguration.Logger.Info.Invoke(sprintf "Sync for group '%s' failed with %O. Trying to rejoin..." options.GroupId errorCode)
                        return! reconnectState()
                    | ErrorCode.GroupAuthorizationFailedCode ->
                        setFatalException (InvalidOperationException(sprintf "Not authorized to join group '%s'" options.GroupId))
                    | _ ->
                        let ex = InvalidOperationException(sprintf "Got unexpected error code, while trying to join group '%s'. Trying to rejoin..." options.GroupId)
                        LogConfiguration.Logger.Error.Invoke(ex.Message, ex)
                        return! reconnectState()
                }
            and syncingState (generationId : Id) (memberId : string) (coordinatorId : Id) (response : SyncGroupResponse) =
                async {
                    try
                        updateConsumerPosition response.MemberAssignment.PartitionAssignment
                    with
                        e ->
                            LogConfiguration.Logger.Warning.Invoke("Got unexpected exception while updating consumer offsets. Trying to rejoin...", e)
                            return! reconnectState()
                    LogConfiguration.Logger.Trace.Invoke(sprintf "Connected to group '%s' with assignment %A" options.GroupId response.MemberAssignment.PartitionAssignment)
                    let cts = new CancellationTokenSource()
                    Async.Start(consumeAsync(cts.Token), cts.Token)
                    return! connectedState cts generationId memberId coordinatorId
                }
            and connectedState (cts : CancellationTokenSource) (generationId : Id) (memberId : string) (coordinatorId : Id) =
                async {
                    let! msg = inbox.TryReceive(options.HeartbeatInterval)
                    match msg with
                    | Some x ->
                        match x with
                        | LeaveGroup ->
                            stopConsuming cts
                            return! notConnectedState()
                        | _ ->
                            return! sendHeartbeatRequestAsync generationId memberId coordinatorId cts connectedState failedHeartbeatState
                    | None ->
                        if agentCts.IsCancellationRequested then
                            stopConsuming cts
                        else
                            return! sendHeartbeatRequestAsync generationId memberId coordinatorId cts connectedState failedHeartbeatState
                }
            and failedHeartbeatState (cts : CancellationTokenSource) (errorCode : ErrorCode) =
                async {
                    stopConsuming cts
                    match errorCode with
                    | ErrorCode.ConsumerCoordinatorNotAvailable
                    | ErrorCode.NotCoordinatorForConsumer
                    | ErrorCode.IllegalGenerationCode
                    | ErrorCode.UnknownMemberIdCode ->
                        LogConfiguration.Logger.Trace.Invoke(sprintf "Heartbeat failed with %O. Trying to rejoin..." errorCode)
                        return! reconnectState()
                    | ErrorCode.RebalanceInProgressCode ->
                        return! reconnectState()
                    | ErrorCode.GroupAuthorizationFailedCode ->
                        setFatalException (InvalidOperationException(sprintf "Not authorized to join group '%s'" options.GroupId))
                    | _ ->
                        let ex = InvalidOperationException(sprintf "Got unexpected error code, while trying to join group '%s'. Trying to rejoin..." options.GroupId)
                        LogConfiguration.Logger.Error.Invoke(ex.Message, ex)
                        return! reconnectState()
                }

            notConnectedState())

    new (brokerSeeds, options : GroupConsumerOptions) = new GroupConsumer(new BrokerRouter(brokerSeeds, options.TcpTimeout), options)

    /// Consume messages from the topic specified. Uses the IConsumer provided in the constructor to consume messages.
    member __.Consume(token) =
        raiseIfDisposed disposed
        agent.Post(JoinGroup token)
        messageQueue

    /// Gets the offset manager
    member __.OffsetManager = consumer.OffsetManager
    
    /// Gets the consumer position
    member __.GetPosition = consumer.GetPosition
    
    /// Sets the consumer position
    member __.SetPosition = consumer.SetPosition
    
    /// Leave the joined group
    member __.LeaveGroup() =
        raiseIfDisposed disposed
        agent.Post(LeaveGroup)

    /// Releases all connections and disposes the consumer
    member __.Dispose() =
        if not disposed then
            groupCts.Cancel()
            consumer.Dispose()
            disposed <- true

    /// Gets the broker router
    member __.BrokerRouter =
        raiseIfDisposed disposed
        consumer.BrokerRouter

    interface IConsumer with
        member self.GetPosition() = self.GetPosition()
        member self.SetPosition(offsets) = self.SetPosition(offsets)
        member self.OffsetManager = self.OffsetManager
        member self.Consume(cancellationToken) = self.Consume(cancellationToken)
        member self.Dispose() = self.Dispose()
        member self.BrokerRouter = self.BrokerRouter