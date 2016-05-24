namespace Franz

open System
open Franz.Internal
open System.Net.Sockets
open System.IO
open Franz.Stream
open Franz.Zookeeper

type NoBrokerFoundForTopicPartitionException (topic : string, partition : int) =
    inherit Exception()
    member __.Topic = topic
    member __.Partition = partition
    override e.Message = sprintf "Could not find broker for topic %s partition %i after several retries." e.Topic e.Partition

type UnableToConnectToAnyBrokerException() =
    inherit Exception()
    override __.Message = "Could not connect to any of the broker seeds"

/// Extensions to help determine outcome of error codes
[<AutoOpen>]
module ErrorCodeExtensions =
    type Messages.ErrorCode with
        /// Check if error code is an real error
        member self.IsError() =
            self <> Messages.ErrorCode.NoError && self <> Messages.ErrorCode.ReplicaNotAvailable
        /// Check if error code is success
        member self.IsSuccess() =
            not <| self.IsError()

/// Type containing which nodes is leaders for which topic and partition
type TopicPartitionLeader = { TopicName : string; PartitionIds : Id array }

/// Broker information and actions
type Broker(brokerId : Id, endPoint : EndPoint, leaderFor : TopicPartitionLeader array, tcpTimeout : int) =
    let _sendLock = new Object()
    let mutable disposed = false
    let mutable client : TcpClient = null
    let send (self : Broker) (request : Request<'TResponse>) =
        try
            if client |> isNull then self.Connect()
            let stream = client.GetStream()
            stream |> request.Serialize
            let messageSize = stream |> BigEndianReader.ReadInt32
            LogConfiguration.Logger.Trace.Invoke(sprintf "Received message of size %i" messageSize)
            let buffer = stream |> BigEndianReader.Read messageSize
            new MemoryStream(buffer)
        with
            | _ ->
                client <- null
                reraise()

    /// Check if broker is leader for the specified topic and partition
    member self.IsLeaderFor(topic, partitionId) =
        self.LeaderFor
        |> Seq.filter (fun x -> x.TopicName = topic)
        |> Seq.map (fun x -> x.PartitionIds)
        |> Seq.concat
        |> Seq.exists (fun x -> x = partitionId)
    member internal self.SetAsLeaderFor(topic, partitionId) =
        let topicIndex = self.LeaderFor |> Array.tryFindIndex (fun x -> x.TopicName = topic)
        match topicIndex with
        | Some x ->
            let topicLeader = self.LeaderFor.[x]
            let leaderPartitions = topicLeader.PartitionIds |> Array.append [| partitionId |]
            self.LeaderFor.[x] <- { topicLeader with PartitionIds = leaderPartitions }
        | None ->
            self.LeaderFor <- self.LeaderFor |> Array.append [| { TopicName = topic; PartitionIds = [| partitionId |] } |]
    member internal self.NoLongerLeaderFor(topic, partitionId) =
        let topicIndex = self.LeaderFor |> Array.tryFindIndex (fun x -> x.TopicName = topic)
        match topicIndex with
        | Some x ->
            let topicLeader = self.LeaderFor.[x]
            let leaderPartitions = topicLeader.PartitionIds |> Seq.filter (fun x -> x = partitionId) |> Seq.toArray
            self.LeaderFor.[x] <- { topicLeader with PartitionIds = leaderPartitions }
        | None -> ()
    member internal self.IsLeaderForPartOfTopic(topic) =
        self.LeaderFor |> Seq.exists (fun x -> x.TopicName = topic)
    /// Gets the broker TcpClient
    member __.Client with get() = client
    /// Gets the broker endpoint
    member __.EndPoint with get() = endPoint
    /// Is the TcpClient connected
    member __.IsConnected with get() = client |> isNull |> not && client.Connected
    /// Gets or sets which topic partitions the broker is leader for
    member val LeaderFor = leaderFor with get, set
    /// Gets the node id
    member __.NodeId with get() = brokerId
    /// Connect the broker
    member __.Connect() =
        raiseIfDisposed(disposed)
        LogConfiguration.Logger.Info.Invoke("Creating new tcp connecting...")
        try
            client <- new TcpClient()
            client.ReceiveTimeout <- tcpTimeout
            client.SendTimeout <- tcpTimeout
            client.Connect(endPoint.Address, endPoint.Port)
        with
        | _ ->
            client <- null
            reraise()
    /// Send a request to the broker
    member self.Send(request : Request<'TResponse>) =
        raiseIfDisposed(disposed)
        let rawResponseStream = lock _sendLock (fun () -> 
            try
                send self request
            with
            | :? UnderlyingConnectionClosedException ->
                LogConfiguration.Logger.Info.Invoke("Broker unable to send, since the underlying connection have been closed since last usage.")
                send self request
            | e ->
                LogConfiguration.Logger.Warning.Invoke("Broker unable to send.", e)
                send self request
            )
        request.DeserializeResponse(rawResponseStream)
    /// Closes the connection and disposes the broker
    member __.Dispose() =
        if not disposed then
            try
                client.Close()
            with
            | _ -> ()
            disposed <- true

    interface IDisposable with
        /// Dispose the broker
        member self.Dispose() = self.Dispose()

/// Indicates ok or failure message
type BrokerRouterReturnMessage<'T> =
    private | Ok of 'T
            | Failure of exn

/// Available messages for the broker router
type BrokerRouterMessage =
    private
    /// Add a broker to the list of available brokers
    | AddBroker of Broker
    /// Refresh metadata
    | RefreshMetadata of AsyncReplyChannel<BrokerRouterReturnMessage<unit>>
    /// Get a broker by topic and partition id
    | GetBroker of string * Id * AsyncReplyChannel<BrokerRouterReturnMessage<Broker>>
    /// Get all available brokers
    | GetAllBrokers of AsyncReplyChannel<Broker seq>
    /// Closes the router
    | Close
    /// Connect to the cluster
    | Connect of EndPoint seq * AsyncReplyChannel<unit>

type IBrokerRouter =
    abstract member Connect : unit -> unit
    abstract member GetAllBrokers : unit -> Broker seq
    abstract member GetBroker : string * Id -> Broker
    abstract member TryToSendToBroker : string * Id * Request<'a> -> 'a
    abstract member GetAvailablePartitionIds : string -> Id array

/// The broker router. Handles all logic related to broker metadata and available brokers.
type BrokerRouter(brokerSeeds : EndPoint array, tcpTimeout) as self =
    let mutable disposed = false
    let cts = new System.Threading.CancellationTokenSource()
    let errorEvent = new Event<_>()
    let metadataRefreshed = new Event<_>()
    let getPartitions nodeId response =
        response.TopicMetadata
        |> Seq.map (fun t -> { TopicName = t.Name; PartitionIds = t.PartitionMetadata |> Seq.filter (fun p -> p.ErrorCode.IsSuccess() && p.Leader = nodeId) |> Seq.map (fun p -> p.PartitionId) |> Seq.toArray })
        |> Seq.toArray
    let mapMetadataResponseToBrokers brokers brokerSeeds (response : MetadataResponse) =
        let newBrokers =
            response.Brokers
                |> Seq.map (fun x -> ({ Address = x.Host; Port = x.Port }, x.NodeId))
                |> Seq.filter (fun (x, _) -> brokers |> Seq.exists(fun (b : Broker) -> b.EndPoint = x) |> not)
                |> Seq.map (fun (endPoint, nodeId) -> new Broker(nodeId, endPoint, getPartitions nodeId response, tcpTimeout))
                |> Seq.toList
        brokers |> Seq.iter (fun x -> x.LeaderFor <- getPartitions x.NodeId response)
        if brokers |> Seq.isEmpty && newBrokers |> Seq.isEmpty then
            brokerSeeds |> Seq.map (fun x -> new Broker(-1, x, [||], tcpTimeout) ) |> Seq.toList
        else [ brokers; newBrokers ] |> Seq.concat |> Seq.toList
    let rec innerConnect seeds =
        match seeds with
        | head :: tail ->
            try
                LogConfiguration.Logger.Info.Invoke(sprintf "Connecting to %s:%i..." head.Address head.Port)
                let broker = new Broker(-1, head, [||], tcpTimeout)
                broker.Connect()
                broker.Send(new MetadataRequest([||])) |> mapMetadataResponseToBrokers [] seeds
            with
            | e ->
                LogConfiguration.Logger.Info.Invoke(sprintf "Could not connect to %s:%i due to (%s), retrying." head.Address head.Port e.Message)
                innerConnect tail
        | [] -> raiseWithFatalLog(UnableToConnectToAnyBrokerException())
    let connect brokerSeeds =
        raiseIfDisposed(disposed)
        if brokerSeeds |> isNull then invalidArg "brokerSeeds" "Brokerseeds cannot be null"
        if brokerSeeds |> Seq.isEmpty then invalidArg "brokerSeeds" "At least one broker seed must be supplied"
        innerConnect (brokerSeeds |> Seq.toList) |> Seq.iter (fun x -> self.AddBroker(x))
    let router = Agent.Start((fun inbox ->
        let rec loop brokers lastRoundRobinIndex connected = async {
            let! msg = inbox.Receive()
            match msg with
            | AddBroker broker ->
                LogConfiguration.Logger.Info.Invoke(sprintf "Adding broker %i with endpoint %A" broker.NodeId broker.EndPoint)
                if not broker.IsConnected then broker.Connect()
                let existingBrokers = (brokers |> Seq.filter (fun (x : Broker) -> x.EndPoint <> broker.EndPoint) |> Seq.toList)
                return! loop (broker :: existingBrokers) lastRoundRobinIndex connected
            | RefreshMetadata reply ->
                try
                    let (index, updatedBrokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex)
                    reply.Reply(Ok())
                    return! loop updatedBrokers index connected
                with
                | e ->
                    reply.Reply(Failure e)
                    return! loop brokers lastRoundRobinIndex connected
            | GetBroker (topic, partitionId, reply) ->
                match self.GetBroker(brokers, lastRoundRobinIndex, topic, partitionId) with
                | Ok (broker, index) ->
                    reply.Reply(Ok(broker))
                    return! loop brokers index connected
                | Failure e ->
                    reply.Reply(Failure(e))
                    return! loop brokers lastRoundRobinIndex connected
            | GetAllBrokers reply ->
                reply.Reply(brokers)
                return! loop brokers lastRoundRobinIndex connected
            | Close ->
                brokers |> Seq.iter (fun x -> x.Dispose())
                cts.Cancel()
                disposed <- true
                return! loop brokers lastRoundRobinIndex connected
            | Connect (brokerSeeds, reply) ->
                if not connected then connect brokerSeeds
                reply.Reply()
                return! loop brokers lastRoundRobinIndex true
        }
        loop [] -1 false), cts.Token)

    let rec getMetadata brokers attempt lastRoundRobinIndex topics =
        let (index, broker : Broker) = brokers |> Seq.roundRobin lastRoundRobinIndex
        try
            try
                if not broker.IsConnected then broker.Connect()
                let response = broker.Send(new MetadataRequest(topics))
                (index, response)
            with
            | _ ->
                if not broker.IsConnected then broker.Connect()
                let response = broker.Send(new MetadataRequest(topics))
                (index, response)
        with
        | e ->
            LogConfiguration.Logger.Info.Invoke(sprintf "Unable to get metadata from broker %i due to (%s), retrying." broker.NodeId e.Message)
            if attempt < (brokers |> Seq.length) then getMetadata brokers (attempt + 1) lastRoundRobinIndex topics
            else
                raiseWithFatalLog (UnableToConnectToAnyBrokerException())

    let rec findBroker brokers lastRoundRobinIndex attempt topic partitionId =
        let candidateBrokers = brokers |> Seq.filter (fun (x : Broker) -> x.LeaderFor |> Seq.exists (fun y -> y.TopicName = topic && y.PartitionIds |> Seq.exists (fun id -> id = partitionId)))
        match candidateBrokers |> Seq.length with
        | 0 ->
            LogConfiguration.Logger.Info.Invoke(sprintf "Unable to find broker of %s partition %i... Refreshing metadata..." topic partitionId)
            let (index, brokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex, [| topic |])
            System.Threading.Thread.Sleep(500)
            if attempt < 3 then findBroker brokers index (attempt + 1) topic partitionId
            else
                Failure(NoBrokerFoundForTopicPartitionException(topic, partitionId))
        | _ ->
            let broker = candidateBrokers |> Seq.head
            Ok(broker, lastRoundRobinIndex)

    let refreshMetadataOnException (brokerRouter : BrokerRouter) topicName partitionId (e : exn) =
        LogConfiguration.Logger.Info.Invoke(sprintf "Unable to send request to broker due to (%s), refreshing metadata." e.Message)
        brokerRouter.RefreshMetadata()
        brokerRouter.GetBroker(topicName, partitionId)

    do
        router.Error.Add(fun x -> errorEvent.Trigger(x))
    /// Event used in case of unhandled exception in internal agent
    [<CLIEvent>]
    member __.Error = errorEvent.Publish
    /// Event triggered when metadata is refreshed
    [<CLIEvent>]
    member __.MetadataRefreshed = metadataRefreshed.Publish
    /// Connect the router to the cluster using the broker seeds.
    member __.Connect() = router.PostAndReply(fun reply -> Connect(brokerSeeds, reply))
    /// Refresh metadata for the broker cluster
    member private __.RefreshMetadata(brokers, lastRoundRobinIndex, ?topics) =
        LogConfiguration.Logger.Info.Invoke("Refreshing metadata...")
        let topics =
            match topics with
            | Some x -> x
            | None -> [||]
        let (index, response) = getMetadata brokers 0 lastRoundRobinIndex topics
        let getPartitions nodeId =
            response.TopicMetadata
            |> Seq.map (fun t -> { TopicName = t.Name; PartitionIds = t.PartitionMetadata |> Seq.filter (fun p -> p.ErrorCode.IsSuccess() && p.Leader = nodeId) |> Seq.map (fun p -> p.PartitionId) |> Seq.toArray })
            |> Seq.toArray
        let newBrokers =
            response.Brokers
            |> Seq.map (fun x -> ({ Address = x.Host; Port = x.Port }, x.NodeId))
            |> Seq.filter (fun (x, _) -> brokers |> Seq.exists(fun b -> b.EndPoint = x) |> not)
            |> Seq.map (fun (endPoint, nodeId) -> new Broker(nodeId, endPoint, getPartitions nodeId, tcpTimeout))
        newBrokers
            |> Seq.iter (fun x ->
                try
                    x.Connect()
                with
                | e ->
                    LogConfiguration.Logger.Warning.Invoke(sprintf "Could not connect to NEW broker %A" x, e))
        let nonExistingBrokers =
            newBrokers
            |> Seq.filter (fun x -> x.IsConnected)
            |> Seq.toList
        brokers
            |> Seq.filter (fun x -> response.Brokers |> Seq.exists (fun b -> b.NodeId = x.NodeId))
            |> Seq.iter (fun x -> x.LeaderFor <- getPartitions x.NodeId)
        let updatedBrokers = [ brokers; nonExistingBrokers ] |> Seq.concat |> Seq.toList
        metadataRefreshed.Trigger(updatedBrokers)
        (index, updatedBrokers)
    /// Get broker by topic and partition id
    member private __.GetBroker(brokers, lastRoundRobinIndex, topic, partitionId) =
        findBroker brokers lastRoundRobinIndex 0 topic partitionId
    /// Add broker to the list of available brokers
    member __.AddBroker(broker : Broker) =
        raiseIfDisposed(disposed)

        router.Post(AddBroker(broker))
    /// Refresh cluster metadata
    member __.RefreshMetadata() =
        raiseIfDisposed(disposed)

        match router.PostAndReply(fun reply -> RefreshMetadata(reply)) with
        | Ok _ -> ()
        | Failure e -> raise e
    /// Get all available brokers
    member __.GetAllBrokers() =
        raiseIfDisposed(disposed)

        router.PostAndReply(fun reply -> GetAllBrokers(reply))
    /// Get broker by topic and partition id
    member __.GetBroker(topic, partitionId) =
        raiseIfDisposed(disposed)

        match router.PostAndReply(fun reply -> GetBroker(topic, partitionId, reply)) with
        | Ok x -> x
        | Failure e -> raise e
    /// Try to send a request to broker handling the specified topic and partition.
    /// If an exception occurs while sending the request, the metadata is refreshed and the request is send again.
    /// If this also fails the exception is thrown and should be handled by the caller.
    member self.TrySendToBroker(topicName, partitionId, request) =
        let broker = self.GetBroker(topicName, partitionId)
        Retry.retryOnException broker (refreshMetadataOnException self topicName partitionId) (fun x -> x.Send(request))
    member __.GetAvailablePartitionIds(topicName) =
        raiseIfDisposed(disposed)

        let brokers = router.PostAndReply(fun reply -> GetAllBrokers(reply))
        brokers
            |> Seq.map (fun x -> x.LeaderFor)
            |> Seq.concat
            |> Seq.filter (fun x -> x.TopicName = topicName)
            |> Seq.map (fun x -> x.PartitionIds)
            |> Seq.concat
            |> Seq.toArray
    /// Dispose the router
    member __.Dispose() =
        if not disposed then
            router.Post(Close)
            disposed <- true
    interface IDisposable with
        /// Dispose the router
        member self.Dispose() = self.Dispose()
    interface IBrokerRouter with
        member self.Connect() = self.Connect()
        member self.GetAllBrokers() = self.GetAllBrokers()
        member self.GetAvailablePartitionIds(topicName) = self.GetAvailablePartitionIds(topicName)
        member self.TryToSendToBroker(topicName, partitionId, request) = self.TrySendToBroker(topicName, partitionId, request)
        member self.GetBroker(topic, partitionId) = self.GetBroker(topic, partitionId)
