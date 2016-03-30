namespace Franz

open System
open Franz.Internal
open System.Net.Sockets
open System.IO
open Franz.Stream

/// Extensions to help determine outcome of error codes
[<AutoOpen>]
module ErrorCodeExtensions =
    type ErrorCode with
        /// Check if error code is an real error
        member self.IsError() =
            self <> ErrorCode.NoError && self <> ErrorCode.ReplicaNotAvailable
        /// Check if error code is success
        member self.IsSuccess() =
            not <| self.IsError()

/// A endpoint
type EndPoint = { Address : string; Port : int32 }
/// Type containing which nodes is leaders for which topic and partition
type TopicPartitionLeader = { TopicName : string; PartitionIds : Id array }

/// Broker information and actions
type Broker(nodeId : Id, endPoint : EndPoint, leaderFor : TopicPartitionLeader array, tcpTimeout : int) =
    let _sendLock = new Object()
    let mutable disposed = false
    let mutable client : TcpClient = null
    let send (self : Broker) (request : Request<'TResponse>) =
        if client |> isNull then self.Connect()
        let stream = client.GetStream()
        stream |> request.Serialize
        let messageSize = stream |> BigEndianReader.ReadInt32
        LogConfiguration.Logger.Trace.Invoke(sprintf "Received message of size %i" messageSize)
        let buffer = stream |> BigEndianReader.Read messageSize
        new MemoryStream(buffer)
    /// Gets the broker TcpClient
    member __.Client with get() = client
    /// Gets the broker endpoint
    member __.EndPoint with get() = endPoint
    /// Is the TcpClient connected
    member __.IsConnected with get() = client |> isNull |> not && client.Connected
    /// Gets or sets which topic partitions the broker is leader for
    member val LeaderFor = leaderFor with get, set
    /// Gets the node id
    member __.NodeId with get() = nodeId
    /// Connect the broker
    member __.Connect() =
        if disposed then invalidOp "Broker has been disposed"
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
        if disposed then invalidOp "Broker has been disposed"
        let rawResponseStream = lock _sendLock (fun () -> 
            try
                send self request
            with
            | e ->
                LogConfiguration.Logger.Error.Invoke(sprintf "Got exception while sending request", e)
                LogConfiguration.Logger.Trace.Invoke("Reconnecting...")
                self.Connect()
                try
                    send self request
                with
                | _ ->
                    client <- null
                    reraise()
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
    | GetAllBrokers of AsyncReplyChannel<Broker list>
    /// Closes the router
    | Close
    /// Connect to the cluster
    | Connect of EndPoint seq * AsyncReplyChannel<unit>

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
                LogConfiguration.Logger.Trace.Invoke(sprintf "Connecting to %s:%i..." head.Address head.Port)
                let broker = new Broker(-1, head, [||], tcpTimeout)
                broker.Connect()
                broker.Send(new MetadataRequest([||])) |> mapMetadataResponseToBrokers [] seeds
            with
            | e ->
                LogConfiguration.Logger.Error.Invoke(sprintf "Could not connect to %s:%i got exception %s" head.Address head.Port e.Message, e)
                innerConnect tail
        | [] -> invalidOp "Could not connect to any of the broker seeds"
    let connect brokerSeeds =
        if disposed then invalidOp "Router has been disposed"
        if brokerSeeds |> isNull then invalidArg "brokerSeeds" "Brokerseeds cannot be null"
        if brokerSeeds |> Seq.isEmpty then invalidArg "brokerSeeds" "At least one broker seed must be supplied"
        innerConnect (brokerSeeds |> Seq.toList) |> Seq.iter (fun x -> self.AddBroker(x))
    let router = Agent.Start((fun inbox ->
        let rec loop brokers lastRoundRobinIndex connected = async {
            let! msg = inbox.Receive()
            match msg with
            | AddBroker broker ->
                LogConfiguration.Logger.Trace.Invoke(sprintf "Adding broker with endpoint %A" broker.EndPoint)
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
            LogConfiguration.Logger.Error.Invoke("Got exception while getting metadata", e)
            if attempt < (brokers |> Seq.length) then getMetadata brokers (attempt + 1) lastRoundRobinIndex topics
            else
                LogConfiguration.Logger.Error.Invoke("Could not get metadata as none of the brokers are available", new Exception())
                invalidOp "Could not get metadata as none of the brokers are available"

    let rec findBroker brokers lastRoundRobinIndex attempt topic partitionId =
        let candidateBrokers = brokers |> Seq.filter (fun (x : Broker) -> x.LeaderFor |> Seq.exists (fun y -> y.TopicName = topic && y.PartitionIds |> Seq.exists (fun id -> id = partitionId)))
        match candidateBrokers |> Seq.length with
        | 0 ->
            LogConfiguration.Logger.Trace.Invoke(sprintf "Could not find broker of %s partition %i... Refreshing metadata..." topic partitionId)
            let (index, brokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex, [| topic |])
            System.Threading.Thread.Sleep(500)
            if attempt < 3 then findBroker brokers index (attempt + 1) topic partitionId
            else
                LogConfiguration.Logger.Trace.Invoke(sprintf "Could not find broker for topic %s partition %i" topic partitionId)
                Failure(InvalidOperationException(sprintf "Could not find broker for topic %s partition %i" topic partitionId))
        | _ ->
            let broker = candidateBrokers |> Seq.head
            Ok(broker, lastRoundRobinIndex)

    let refreshMetadataOnException (brokerRouter : BrokerRouter) topicName partitionId (e : exn) =
        LogConfiguration.Logger.Error.Invoke("Got exception while sending request", e)
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
        LogConfiguration.Logger.Trace.Invoke("Refreshing metadata...")
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
                    LogConfiguration.Logger.Error.Invoke(sprintf "Could not connect to broker %A" x, e))
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
    member private self.GetBroker(brokers, lastRoundRobinIndex, topic, partitionId) =
        findBroker brokers lastRoundRobinIndex 0 topic partitionId
    /// Add broker to the list of available brokers
    member __.AddBroker(broker : Broker) =
        if disposed then invalidOp "Router has been disposed"
        router.Post(AddBroker(broker))
    /// Refresh cluster metadata
    member __.RefreshMetadata() =
        if disposed then invalidOp "Router has been disposed"
        match router.PostAndReply(fun reply -> RefreshMetadata(reply)) with
        | Ok _ -> ()
        | Failure e -> raise e
    /// Get all available brokers
    member __.GetAllBrokers() =
        if disposed then invalidOp "Router has been disposed"
        router.PostAndReply(fun reply -> GetAllBrokers(reply))
    /// Get broker by topic and partition id
    member __.GetBroker(topic, partitionId) =
        if disposed then invalidOp "Router has been disposed"
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
        if disposed then invalidOp "Router has been disposed"
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
