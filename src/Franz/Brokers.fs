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
        client <- new TcpClient()
        client.ReceiveTimeout <- tcpTimeout
        client.SendTimeout <- tcpTimeout
        client.Connect(endPoint.Address, endPoint.Port)
    /// Send a request to the broker
    member self.Send(request : Request<'TResponse>) =
        if disposed then invalidOp "Broker has been disposed"
        let rawResponseStream = lock _sendLock (fun () -> 
            let send () =
                if client |> isNull then self.Connect()
                let stream = self.Client.GetStream()
                stream |> request.Serialize
                let messageSize = stream |> BigEndianReader.ReadInt32
                dprintfn "Received message of size %i" messageSize
                let buffer = stream |> BigEndianReader.Read messageSize
                new MemoryStream(buffer)
            try
                send()
            with
            | e ->
                dprintfn "Got exception while sending request: %s" e.Message
                dprintfn "Reconnecting..."
                self.Connect()
                try
                    send()
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
type BrokerRouter(tcpTimeout) as self =
    let mutable disposed = false
    let cts = new System.Threading.CancellationTokenSource()
    let errorEvent = new Event<_>()
    let metadataRefreshed = new Event<_>()
    let connect brokerSeeds =
        if disposed then invalidOp "Router has been disposed"
        if brokerSeeds |> isNull then invalidArg "brokerSeeds" "Brokerseeds cannot be null"
        if brokerSeeds |> Seq.isEmpty then invalidArg "brokerSeeds" "At least one broker seed must be supplied"
        let mapMetadataResponseToBrokers brokers (response : MetadataResponse) =
            let getPartitions nodeId =
                response.TopicMetadata
                |> Seq.map (fun t -> { TopicName = t.Name; PartitionIds = t.PartitionMetadata |> Seq.filter (fun p -> p.ErrorCode.IsSuccess() && p.Leader = nodeId) |> Seq.map (fun p -> p.PartitionId) |> Seq.toArray })
                |> Seq.toArray
            let newBrokers =
                response.Brokers
                    |> Seq.map (fun x -> ({ Address = x.Host; Port = x.Port }, x.NodeId))
                    |> Seq.filter (fun (x, _) -> brokers |> Seq.exists(fun (b : Broker) -> b.EndPoint = x) |> not)
                    |> Seq.map (fun (endPoint, nodeId) -> new Broker(nodeId, endPoint, getPartitions nodeId, tcpTimeout))
                    |> Seq.toList
            brokers |> Seq.iter (fun x -> x.LeaderFor <- getPartitions x.NodeId )
            if brokers |> Seq.isEmpty && newBrokers |> Seq.isEmpty then
                brokerSeeds |> Seq.map (fun x -> new Broker(-1, x, [||], tcpTimeout) ) |> Seq.toList
            else [ brokers; newBrokers ] |> Seq.concat |> Seq.toList
        let rec innerConnect seeds =
            match seeds with
            | head :: tail ->
                try
                    dprintfn "Connecting to %s:%i..." head.Address head.Port
                    let broker = new Broker(-1, head, [||], tcpTimeout)
                    broker.Connect()
                    broker.Send(new MetadataRequest([||])) |> mapMetadataResponseToBrokers []
                with
                | e ->
                    dprintfn "Could not connect to %s:%i got exception %s" head.Address head.Port e.Message
                    innerConnect tail
            | [] -> raise (InvalidOperationException("Could not connect to any of the broker seeds"))
        innerConnect (brokerSeeds |> Seq.toList) |> Seq.iter (fun x -> self.AddBroker(x))
    let router = Agent.Start((fun inbox ->
        let rec loop brokers lastRoundRobinIndex connected = async {
            let! msg = inbox.Receive()
            match msg with
            | AddBroker broker ->
                dprintfn "Adding broker with endpoint %A" broker
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
    do
        router.Error.Add(fun x -> errorEvent.Trigger(x))
    /// Event used in case of unhandled exception in internal agent
    [<CLIEvent>]
    member __.Error = errorEvent.Publish
    /// Event triggered when metadata is refreshed
    [<CLIEvent>]
    member __.MetadataRefreshed = metadataRefreshed.Publish
    /// Connect the router to the cluster using the broker seeds.
    member __.Connect(brokerSeeds) = router.PostAndReply(fun reply -> Connect(brokerSeeds, reply))
    /// Refresh metadata for the broker cluster
    member private __.RefreshMetadata(brokers, lastRoundRobinIndex, ?topics) =
        dprintfn "Refreshing metadata..."
        let topics =
            match topics with
            | Some x -> x
            | None -> [||]
        let (index, response) =
            let rec getMetadata brokers attempt =
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
                    dprintfn "Got exception while getting metadata: %s" e.Message
                    if attempt < (brokers |> Seq.length) then getMetadata brokers (attempt + 1)
                    else invalidOp "Could not get metadata as none of the brokers are available"
            getMetadata brokers 0
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
                    dprintfn "Could not connect to broker %s:%i, exception %s" x.EndPoint.Address x.EndPoint.Port e.Message)
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
        let rec find brokers lastRoundRobinIndex attempt =
            let candidateBrokers = brokers |> Seq.filter (fun (x : Broker) -> x.LeaderFor |> Seq.exists (fun y -> y.TopicName = topic && y.PartitionIds |> Seq.exists (fun id -> id = partitionId)))
            match candidateBrokers |> Seq.length with
            | 0 ->
                dprintfn "Could not find broker of %s partition %i... Refreshing metadata..." topic partitionId
                let (index, brokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex, [| topic |])
                System.Threading.Thread.Sleep(500)
                if attempt < 3 then find brokers index (attempt + 1)
                else Failure(InvalidOperationException(sprintf "Could not find broker for topic %s partition %i" topic partitionId))
            | _ ->
                let broker = candidateBrokers |> Seq.head
                Ok(broker, lastRoundRobinIndex)
        find brokers lastRoundRobinIndex 0
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
    /// Dispose the router
    member __.Dispose() =
        if not disposed then
            router.Post(Close)
            disposed <- true
    interface IDisposable with
        /// Dispose the router
        member self.Dispose() = self.Dispose()
