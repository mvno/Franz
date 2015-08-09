namespace Franz

open System
open Franz.Internal
open System.Net.Sockets

[<AutoOpen>]
module ErrorCodeExtensions =
    type ErrorCode with
        /// Check if error code is an real error
        member self.IsError() =
            self <> ErrorCode.NoError && self <> ErrorCode.ReplicaNotAvailable
        /// Check if error code is success
        member self.IsSuccess() =
            not <| self.IsError()

type EndPoint = { Address : string; Port : int32 }
/// Type containing which nodes is leaders for which topic and partition
type TopicPartitionLeader = { TopicName : string; PartitionIds : Id array }

/// Broker information and actions
type Broker(nodeId : Id, endPoint : EndPoint, leaderFor : TopicPartitionLeader array) =
    let _sendLock = new Object()
    let mutable client : TcpClient = null
    /// Gets the broker TcpClient
    member __.Client with get() = client
    /// Gets the broker endpoint
    member __.EndPoint with get() = endPoint
    /// Is the TcpClient connected
    member __.IsConnected with get() = client <> null && client.Connected
    /// Gets or sets which topic partitions the broker is leader for
    member val LeaderFor = leaderFor with get, set
    /// Gets the node id
    member __.NodeId with get() = nodeId
    /// Connect the broker
    member __.Connect() =
        client <- new TcpClient()
        client.Connect(endPoint.Address, endPoint.Port)
    /// Send a request to the broker
    member self.Send(request : Request<'TResponse>) =
        lock _sendLock (fun () -> 
            let send () =
                let stream = self.Client.GetStream()
                stream |> request.Serialize
                request.DeserializeResponse(stream)
            try
                send()
            with
            | _ ->
                dprintfn "Got exception while sending request... Trying again..."
                self.Connect()
                send())

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
    | RefreshMetadata of AsyncReplyChannel<unit>
    /// Get a broker by topic and partition id
    | GetBroker of string * Id * AsyncReplyChannel<BrokerRouterReturnMessage<Broker>>
    /// Get all available brokers
    | GetAllBrokers of AsyncReplyChannel<Broker list>

/// The broker router. Handles all logic related to broker metadata and available brokers.
type BrokerRouter() as self =
    let errorEvent = new Event<_>()
    let metadataRefreshed = new Event<_>()
    let router = Agent.Start(fun inbox ->
        let rec loop brokers lastRoundRobinIndex = async {
            let! msg = inbox.Receive()
            match msg with
            | AddBroker broker ->
                dprintfn "Adding broker with endpoint %A" broker
                if not broker.IsConnected then broker.Connect()
                let existingBrokers = (brokers |> Seq.filter (fun (x : Broker) -> x.EndPoint <> broker.EndPoint) |> Seq.toList)
                do! loop (broker :: existingBrokers) lastRoundRobinIndex
            | RefreshMetadata reply ->
                let (index, updatedBrokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex)
                reply.Reply()
                do! loop updatedBrokers index
            | GetBroker (topic, partitionId, reply) ->
                match self.GetBroker(brokers, lastRoundRobinIndex, topic, partitionId) with
                | Ok (broker, index) ->
                    reply.Reply(Ok(broker))
                    do! loop brokers index
                | Failure e ->
                    reply.Reply(Failure(e))
                    do! loop brokers lastRoundRobinIndex
            | GetAllBrokers reply ->
                reply.Reply(brokers)
                do! loop brokers lastRoundRobinIndex
        }
        loop [] -1)
    do
        router.Error.Add(fun x -> errorEvent.Trigger(x))
    /// Event used in case of unhandled exception in internal agent
    [<CLIEvent>]
    member __.Error = errorEvent.Publish
    /// Event triggered when metadata is refreshed
    [<CLIEvent>]
    member __.MetadataRefreshed = metadataRefreshed.Publish
    /// Connect the router to the cluster using the broker seeds.
    member self.Connect(brokerSeeds) =
        if brokerSeeds = null then invalidArg "brokerSeeds" "Brokerseeds cannot be null"
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
                    |> Seq.map (fun (endPoint, nodeId) -> new Broker(nodeId, endPoint, getPartitions nodeId))
                    |> Seq.toList
            brokers |> Seq.iter (fun x -> x.LeaderFor <- getPartitions x.NodeId )
            [ brokers; newBrokers ] |> Seq.concat |> Seq.toList
        let rec connect seeds =
            match seeds with
            | head :: tail ->
                try
                    dprintfn "Connecting to %s:%i..." head.Address head.Port
                    let broker = new Broker(-1, head, [||])
                    broker.Connect()
                    broker.Send(new MetadataRequest([||])) |> mapMetadataResponseToBrokers []
                with
                | e ->
                    dprintfn "Could not connect to %s:%i got exception %s" head.Address head.Port e.Message
                    connect tail
            | [] -> raise (InvalidOperationException("Could not connect to any of the broker seeds"))
        connect (brokerSeeds |> Seq.toList) |> Seq.iter (fun x -> self.AddBroker(x))
    /// Refresh metadata for the broker cluster
    member private __.RefreshMetadata(brokers, lastRoundRobinIndex) =
        dprintfn "Refreshing metadata..."
        let (index, response) =
            let rec getMetadata brokers attempt =
                let (index, broker : Broker) = brokers |> Seq.roundRobin lastRoundRobinIndex
                try
                    try
                        if not broker.IsConnected then broker.Connect()
                        let response = broker.Send(new MetadataRequest([||]))
                        (index, response)
                    with
                    | _ ->
                        if not broker.IsConnected then broker.Connect()
                        let response = broker.Send(new MetadataRequest([||]))
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
            |> Seq.map (fun (endPoint, nodeId) -> new Broker(nodeId, endPoint, getPartitions nodeId))
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
                let (index, brokers) = self.RefreshMetadata(brokers, lastRoundRobinIndex)
                System.Threading.Thread.Sleep(500)
                if attempt < 3 then find brokers index (attempt + 1)
                else Failure(InvalidOperationException(sprintf "Could not find broker for topic %s partition %i" topic partitionId))
            | _ ->
                let broker = candidateBrokers |> Seq.head
                Ok(broker, lastRoundRobinIndex)
        find brokers lastRoundRobinIndex 0
    /// Add broker to the list of available brokers
    member __.AddBroker(broker : Broker) =
        router.Post(AddBroker(broker))
    /// Refresh cluster metadata
    member __.RefreshMetadata() =
        router.PostAndReply(fun reply -> RefreshMetadata(reply))
    /// Get all available brokers
    member __.GetAllBrokers() =
        router.PostAndReply(fun reply -> GetAllBrokers(reply))
    /// Get broker by topic and partition id
    member __.GetBroker(topic, partitionId) =
        match router.PostAndReply(fun reply -> GetBroker(topic, partitionId, reply)) with
        | Ok x -> x
        | Failure e -> raise e
