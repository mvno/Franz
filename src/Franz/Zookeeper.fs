namespace Franz.Zookeeper

open Franz.Stream
open Franz.Internal
open System.IO
open System.Threading
open Franz
open Franz.Internal.ErrorHandling
open System
open System.Collections.Concurrent

type RequestType =
    | Close = -11
    | Ping = 11
    | GetChildren2 = 12
    | GetData = 4

type ErrorCode =
    | Ok = 0
    | SystemError = -1
    | RuntimeInconsistency = -2
    | DataInconsistency = -3
    | ConnectionLoss = -4
    | MarshallingError = -5
    | Unimplemented = -6
    | OperationTimeout = -7
    | BadArguments = -8
    | ApiError = -100
    | NoNode = -101
    | NoAuth = -102
    | BadVersion = -103
    | NoChildrenForEphemerals = -108
    | NodeExists = -110
    | NotEmpty = -111
    | SessionExpired = -112
    | InvalidCallback = -113
    | InvalidAcl = -114
    | AuthFailed = -115
    | SessionMoved = -116

type Xid = int
type Zxid = int64
type Time = int64

module XidConstants =
    [<Literal>]
    let Ping : Xid = -2
    [<Literal>]
    let Notification : Xid = -1

type ConnectResponse(protocolVersion : int, timeout : int, sessionId : int64, password : byte array) =
    member __.ProtocolVersion = protocolVersion
    member __.Timeout = timeout
    member __.SessionId = sessionId
    member __.Password = password
    static member Deserialize(stream) =
        stream |> BigEndianReader.ReadInt32 |> ignore
        let protocolVersion = stream |> BigEndianReader.ReadInt32
        let timeout = stream |> BigEndianReader.ReadInt32
        let sessionId = stream |> BigEndianReader.ReadInt64
        let password = stream |> BigEndianReader.ReadBytes
        new ConnectResponse(protocolVersion, timeout, sessionId, password)

type ReplyHeader(xid : Xid, zxid : Zxid, error : ErrorCode) =
    member __.Xid = xid
    member __.Zxid = zxid
    member __.Error = error
    static member Deserialize(stream) =
        new ReplyHeader(stream |> BigEndianReader.ReadInt32, stream |> BigEndianReader.ReadInt64, stream |> BigEndianReader.ReadInt32 |> enum<ErrorCode>)

type Stat =
    { CreatedZxid : Zxid; LastModifiedZxid : Zxid; CreatedTime : Time; ModifiedTime : Time; Version : int; ChildVersion : int; AclVersion : int; EphemeralOwner : int64; DataLength : int; NumberOfChildren : int; LastModifiedChildrenZxid : Zxid }
    static member Deserialize(stream) =
        {
            CreatedZxid = stream |> BigEndianReader.ReadInt64;
            LastModifiedZxid = stream |> BigEndianReader.ReadInt64;
            CreatedTime = stream |> BigEndianReader.ReadInt64;
            ModifiedTime = stream |> BigEndianReader.ReadInt64;
            Version = stream |> BigEndianReader.ReadInt32;
            ChildVersion = stream |> BigEndianReader.ReadInt32;
            AclVersion = stream |> BigEndianReader.ReadInt32;
            EphemeralOwner = stream |> BigEndianReader.ReadInt64;
            DataLength = stream |> BigEndianReader.ReadInt32;
            NumberOfChildren = stream |> BigEndianReader.ReadInt32;
            LastModifiedChildrenZxid = stream |> BigEndianReader.ReadInt64;
        }

type GetChildrenResponse(header : ReplyHeader, children : string array, stat : Stat) =
    member __.Children = children
    member __.Stat = stat;
    member __.Header = header
    static member private readChildren list count stream : string list =
        match count with
        | 0 -> list
        | _ ->
            let child = stream |> BigEndianReader.ReadZookeeperString
            GetChildrenResponse.readChildren (child :: list) (count - 1) stream
    static member Deserialize(replyHeader, stream) =
        let numberOfChildren = stream |> BigEndianReader.ReadInt32;
        let children = GetChildrenResponse.readChildren [] numberOfChildren stream
        new GetChildrenResponse(replyHeader, children |> List.toArray, stream |> Stat.Deserialize)

type PingResponse(header : ReplyHeader) =
    member __.Header = header
    static member Deserialize(replyHeader, _) =
        new PingResponse(replyHeader)

type GetDataResponse(header : ReplyHeader, data : byte array) =
    member __.Header = header
    member __.Data = data
    static member Deserialize(replyHeader, stream) =
        let data = stream |> BigEndianReader.ReadBytes
        new GetDataResponse(replyHeader, data)

type EventType =
    | None = -1
    | NodeCreated = 1
    | NodeDeleted = 2
    | NodeDataChanged = 3
    | NodeChildrenChanged = 4

type WatcherEvent(header : ReplyHeader, eventType : EventType, state : int, path : string) =
    member __.Header = header
    member __.EventType = eventType
    member __.State = state
    member __.Path = path
    static member Deserialize(replyHeader, stream : Stream) =
        let eventType = stream |> BigEndianReader.ReadInt32
        let state = stream |> BigEndianReader.ReadInt32
        let path = stream |> BigEndianReader.ReadZookeeperString
        new WatcherEvent(replyHeader, eventType |> enum<EventType>, state, path)

type IRequest =
    abstract member Serialize : Xid -> byte array

[<AbstractClass; Sealed>]
type RequestHeader() =
    static member Serialize(xid : Xid, requestType : RequestType) = 
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 xid
        ms |> BigEndianWriter.WriteInt32 (requestType |> int)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let size = ms.Length |> int
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer

type PingRequest() =
    member __.Serialize(_) =
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 0
        BigEndianWriter.Write ms (RequestHeader.Serialize(XidConstants.Ping, RequestType.Ping))
        let size = int32 ms.Length
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        ms |> BigEndianWriter.WriteInt32 (size - 4)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer
    interface IRequest with
        member self.Serialize(xid) = self.Serialize(xid)

type ConnectRequest(protocolVersion, lastZxid, timeout, sessionId, password) =
    member val ProtocolVersion = protocolVersion with get
    member val LastZxid = lastZxid with get
    member val Timeout = timeout with get
    member val SessionId = sessionId with get
    member val Password = password with get
    member self.Serialize(_) =
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 0
        ms |> BigEndianWriter.WriteInt32 self.ProtocolVersion
        ms |> BigEndianWriter.WriteInt64 self.LastZxid
        ms |> BigEndianWriter.WriteInt32 self.Timeout
        ms |> BigEndianWriter.WriteInt64 self.SessionId
        ms |> BigEndianWriter.WriteBytes self.Password
        let size = int32 ms.Length
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        ms |> BigEndianWriter.WriteInt32 (size - 4)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer
    interface IRequest with
        member self.Serialize(xid) = self.Serialize(xid)

type GetChildrenRequest(path : string, watch : bool) =
    member __.Path = path
    member __.Watch = watch
    member __.Serialize(xid) =
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 0
        BigEndianWriter.Write ms (RequestHeader.Serialize(xid, RequestType.GetChildren2))
        ms |> BigEndianWriter.WriteZookeeperString path
        ms |> BigEndianWriter.WriteInt8 ((if watch then 1 else 0) |> int8)
        let size = int32 ms.Length
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        ms |> BigEndianWriter.WriteInt32 (size - 4)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer
    interface IRequest with
        member self.Serialize(xid) = self.Serialize(xid)

type GetDataRequest(path : string, watch : bool) =
    member __.Path = path
    member __.Serialize(xid) =
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 0
        BigEndianWriter.Write ms (RequestHeader.Serialize(xid, RequestType.GetData))
        ms |> BigEndianWriter.WriteZookeeperString path
        ms |> BigEndianWriter.WriteInt8 ((if watch then 1 else 0) |> int8)
        let size = int32 ms.Length
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        ms |> BigEndianWriter.WriteInt32 (size - 4)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer
    interface IRequest with
        member self.Serialize(xid) = self.Serialize(xid)

type ResponsePacket = { Content : Stream; Header : ReplyHeader }

type WatcherType =
    private
    | Child
    | Data

type Watcher =
    { Path : string; Callback : unit -> unit; Type : WatcherType }
    member self.Reregister(agent : Agent<ZookeeperMessages>) =
        match self.Type with
        | Child -> agent.PostAndReply(fun reply -> GetChildrenWithWatcher(self.Path, self, reply))
        | Data -> agent.PostAndReply(fun reply -> GetDataWithWatcher(self.Path, self, reply))
        |> either (fun x -> x |> Async.Ignore |> Async.Start) (fun x -> LogConfiguration.Logger.Error.Invoke(sprintf "Could not reregister watcher for %s" self.Path, x))
and ZookeeperMessages =
    private
    | Connect of string * int * int * AsyncReplyChannel<Result<unit, exn>>
    | GetChildrenWithWatcher of string * Watcher * AsyncReplyChannel<Result<Async<ResponsePacket>, exn>>
    | GetChildren of string * AsyncReplyChannel<Result<Async<ResponsePacket>, exn>>
    | Ping
    | GetData of string * AsyncReplyChannel<Result<Async<ResponsePacket>, exn>>
    | GetDataWithWatcher of string * Watcher * AsyncReplyChannel<Result<Async<ResponsePacket>, exn>>
    | Reconnect of TcpClient.T
    | Dispose

type RequestPacket(xid) =
    let mutable response = None
    let waiter = new System.Threading.ManualResetEventSlim()
    member __.Xid = xid
    member __.GetResponseAsync(timeout : int) =
        async {
            let success = waiter.Wait(timeout)
            if not success then failwith "Response not received in time"
            return response.Value
        }
    member internal __.ResponseReceived(packet) =
        response <- Some packet
        waiter.Set()

type private Pinger = { Body : Async<unit>; CancellationTokenSource : CancellationTokenSource }
type private State = { TcpClient : TcpClient.T; SessionId : int64; Password : byte array; LastXid : Xid; SessionTimeout : int; Receiver : Async<unit> option; Pinger : Pinger option }

type ZookeeperClient(connectionLossCallback : Action) =
    let mutable disposed = false
    let deserialize f response =
        f(response.Header, response.Content)
    let createPinger sessionTimeout (agent : Agent<ZookeeperMessages>) =
        let cts = new CancellationTokenSource()
        let body =
            async {
                while not cts.Token.IsCancellationRequested do
                    do! Async.Sleep (sessionTimeout / 2)
                    if cts.Token.IsCancellationRequested then return ()
                    else agent.Post(Ping)
            }
        body |> Async.Start
        { Body = body; CancellationTokenSource = cts }

    let agent = Agent.Start(fun inbox ->
        let lastZxid = ref 0L
        let pendingRequests = new ConcurrentQueue<RequestPacket>()
        let childWatchers = new ConcurrentDictionary<string, Watcher>()
        let dataWatchers = new ConcurrentDictionary<string, Watcher>()

        let sendNewRequest (request : IRequest) state =
            let xid = state.LastXid + 1
            let requestPacket = new RequestPacket(xid)
            pendingRequests.Enqueue(requestPacket)
            state.TcpClient |> TcpClient.write (request.Serialize(xid)) |> ignore
            ({ state with LastXid = state.LastXid + 1}, requestPacket.GetResponseAsync(state.SessionTimeout))

        let tryToOption (success, x) = if success then Some x else None

        let rec receive (stream : Stream) =
            let responseSize = stream |> BigEndianReader.ReadInt32
            let ms = new MemoryStream(stream |> BigEndianReader.Read responseSize)
            let header = ms |> ReplyHeader.Deserialize
            let (success, request) = pendingRequests.TryDequeue()
            if not success && header.Xid <> XidConstants.Ping && header.Xid <> XidConstants.Notification then
                raise (new IOException(sprintf "Nothing in queue, but got xid %i" header.Xid))
            if header.Zxid > 0L then Interlocked.Increment(lastZxid) |> ignore
            if header.Error <> ErrorCode.Ok then raise (new IOException(sprintf "Got error code %A" header.Error))
            match header.Xid with
            | XidConstants.Ping -> LogConfiguration.Logger.Trace.Invoke("Got ping response")
            | XidConstants.Notification ->
                let response = WatcherEvent.Deserialize(header, ms)
                let watcher, suppress =
                    match response.EventType with
                    | EventType.NodeChildrenChanged -> (childWatchers.TryGetValue(response.Path) |> tryToOption, false)
                    | EventType.NodeDataChanged -> (dataWatchers.TryGetValue(response.Path) |> tryToOption, false)
                    | EventType.NodeDeleted | EventType.NodeCreated -> (None, true)
                    | _ ->
                        LogConfiguration.Logger.Warning.Invoke("Got unsupported notification")
                        (None, false)
                match watcher with
                | Some x ->
                    x.Reregister(inbox)
                    x.Callback()
                | None when not suppress -> LogConfiguration.Logger.Warning.Invoke(sprintf "Got notification for '%A', but cannot find a matching watcher" response.Path)
                | _ -> ()
            | x when x = request.Xid ->
                LogConfiguration.Logger.Trace.Invoke(sprintf "Received with xid %i" header.Xid)
                { Header = header; Content = ms } |> request.ResponseReceived
            | _ ->
                raise (new IOException(sprintf "Xid out of order. Got %i expected %i" header.Xid request.Xid))
            receive stream

        let replyEither (reply : AsyncReplyChannel<Result<'a, exn>>) oldState result : State =
            match result with
            | Success (state, x) -> reply.Reply(x |> succeed); state
            | Failure x -> reply.Reply(x |> fail); oldState

        let sendConnectRequest sessionTimeout sessionId password state =
            let request = new ConnectRequest(0, lastZxid.Value, sessionTimeout, sessionId, password)
            let response =
                state.TcpClient
                |> TcpClient.write (request.Serialize(-1))
                |> TcpClient.read ConnectResponse.Deserialize
            let pinger = inbox |> createPinger sessionTimeout
            let receiver = async {
                try
                    do receive state.TcpClient.Stream.Value
                with
                | _ ->
                    pinger.CancellationTokenSource.Cancel()
                    inbox.Post(Reconnect(state.TcpClient))
            }
            receiver |> Async.Start
            { state with SessionId = response.SessionId; Password = response.Password; SessionTimeout = sessionTimeout; Receiver = Some receiver; Pinger = Some pinger }

        let connect host port sessionTimeout state =
            { state with TcpClient = state.TcpClient |> TcpClient.connectTo host port }
            |> sendConnectRequest sessionTimeout (int64 0) (Array.zeroCreate(16))

        let getChildren path watcher state =
            let shouldWatch = if watcher |> Option.isSome then true else false
            let request = new GetChildrenRequest(path, shouldWatch)
            if shouldWatch then childWatchers.TryAdd(path, watcher.Value) |> ignore
            sendNewRequest request state

        let getChildrenWithoutWatcher path = getChildren path None

        let getData path watcher state =
            let shouldWatch = if watcher |> Option.isSome then true else false
            let request = new GetDataRequest(path, shouldWatch)
            if shouldWatch then dataWatchers.TryAdd(path, watcher.Value) |> ignore
            sendNewRequest request state

        let getDataWithoutWatcher path = getData path None

        let ping state =
            state.TcpClient |> TcpClient.write ((new PingRequest()).Serialize(XidConstants.Ping)) |> ignore

        let reconnect (oldClient : TcpClient.T) state =
            if not <| oldClient.Client.Equals(state.TcpClient.Client) then
                LogConfiguration.Logger.Trace.Invoke("Not reconnecting as state has changed since requested")
                state
            else
                let reconnect state =
                    let newClient = state.TcpClient |> TcpClient.reconnect
                    sendConnectRequest state.SessionTimeout state.SessionId state.Password { state with TcpClient = newClient }
                catch reconnect state
                |> either (fun x -> x) (fun x -> LogConfiguration.Logger.Error.Invoke("Could not reconnect", x); connectionLossCallback.Invoke(); state)

        let rec loop state = async {
            let! msg = inbox.Receive()
            match msg with
            | Connect (host, port, sessionTimeout, reply) ->
                return! loop
                    (catch (connect host port sessionTimeout) state
                    |> either (fun newState -> reply.Reply(() |> succeed); newState) (fun x -> reply.Reply(x |> fail); state))
            | GetChildrenWithWatcher (path, watcher, reply) ->
                return! loop (catch (getChildren path (Some watcher)) state |> replyEither reply state)
            | GetChildren (path, reply) ->
                return! loop (catch (getChildrenWithoutWatcher path) state |> replyEither reply state)
            | Ping ->
                return! loop
                    (catch ping state
                    |> either (fun () -> state) (fun x -> LogConfiguration.Logger.Error.Invoke("Could not ping", x); state))
            | GetData (path, reply) ->
                return! loop (catch (getDataWithoutWatcher path) state |> replyEither reply state)
            | GetDataWithWatcher (path, watcher, reply) ->
                return! loop (catch (getData path (Some watcher)) state |> replyEither reply state)
            | Reconnect oldClient ->
                return! loop (reconnect oldClient state)
            | Dispose ->
                LogConfiguration.Logger.Info.Invoke("Zookeeper agent was disposed")
                ()
        }
        loop { TcpClient = TcpClient.create(); SessionId = 0L; Password = Array.empty; LastXid = 0; SessionTimeout = 0; Receiver = None; Pinger = None })

    let handleAsyncReply fSuccess (result : Result<Async<'a>, exn>) =
        match result with
        | Success x -> x |> Async.RunSynchronously |> fSuccess
        | Failure x -> raise (new Exception(x.Message, x))

    do
        agent.Error.Add(fun x -> LogConfiguration.Logger.Fatal.Invoke("Got exception in zookeeper agent", x))

    member __.Connect(host, port, sessionTimeout) =
        if disposed then invalidOp "Client has been disposed"
        let result = agent.PostAndReply(fun reply -> Connect(host, port, sessionTimeout, reply))
        match result with
        | Failure x -> raise x
        | Success _ -> ()
    member __.GetChildren(path, watcherCallback) =
        if disposed then invalidOp "Client has been disposed"
        let watcher = { Path = path; Callback = watcherCallback; Type = Child }
        agent.PostAndReply(fun reply -> GetChildrenWithWatcher(path, watcher, reply))
        |> handleAsyncReply (deserialize GetChildrenResponse.Deserialize)
    member __.GetChildren(path) =
        if disposed then invalidOp "Client has been disposed"
        agent.PostAndReply(fun reply -> GetChildren(path, reply))
        |> handleAsyncReply (deserialize GetChildrenResponse.Deserialize)
    member __.GetData(path) =
        if disposed then invalidOp "Client has been disposed"
        agent.PostAndReply(fun reply -> GetData(path, reply))
        |> handleAsyncReply (deserialize GetDataResponse.Deserialize)
    member __.GetData(path, watcherCallback) =
        if disposed then invalidOp "Client has been disposed"
        let watcher = { Path = path; Callback = watcherCallback; Type = Data }
        agent.PostAndReply(fun reply -> GetDataWithWatcher(path, watcher, reply))
        |> handleAsyncReply (deserialize GetDataResponse.Deserialize)
    interface IDisposable with
        member __.Dispose() =
            agent.Post(Dispose)
            disposed <- true
