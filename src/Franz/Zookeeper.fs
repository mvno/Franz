namespace Franz.Zookeeper

open Franz.Stream
open Franz.Internal
open System.IO
open System.Threading
open Franz

type RequestType =
    | Close = -11
    | Ping = 11
    | GetChildren2 = 12
    | GetData = 4

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

type ReplyHeader(xid : Xid, zxid : Zxid, error : int) =
    member __.Xid = xid
    member __.Zxid = zxid
    member __.Error = error
    static member Deserialize(stream) =
        new ReplyHeader(stream |> BigEndianReader.ReadInt32, stream |> BigEndianReader.ReadInt64, stream |> BigEndianReader.ReadInt32)

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

type WatcherEvent(header : ReplyHeader, eventType : int, state : int, path : string) =
    member __.Header = header
    member __.EventType = eventType
    member __.State = state
    member __.Path = path
    static member Deserialize(replyHeader, stream : Stream) =
        let eventType = stream |> BigEndianReader.ReadInt32
        let state = stream |> BigEndianReader.ReadInt32
        let path = stream |> BigEndianReader.ReadZookeeperString
        new WatcherEvent(replyHeader, eventType, state, path)

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

type GetDataRequest(path : string) =
    member __.Path = path
    member __.Serialize(xid) =
        let ms = new MemoryStream()
        ms |> BigEndianWriter.WriteInt32 0
        BigEndianWriter.Write ms (RequestHeader.Serialize(xid, RequestType.GetData))
        ms |> BigEndianWriter.WriteZookeeperString path
        ms |> BigEndianWriter.WriteInt8 (0 |> int8)
        let size = int32 ms.Length
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        ms |> BigEndianWriter.WriteInt32 (size - 4)
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(size)
        ms.Read(buffer, 0, size) |> ignore
        buffer

type ResponsePacket = { Content : Stream; Header : ReplyHeader }

type private Result<'a, 'b> =
    | Success of 'a
    | Error of 'b

type Watcher = { Path : string; Callback : unit -> unit }

type ZookeeperMessages =
    private
    | Connect of string * int * int * AsyncReplyChannel<Result<unit, exn>>
    | GetChildrenWithWatcher of string * Watcher * AsyncReplyChannel<Async<ResponsePacket>>
    | GetChildren of string * AsyncReplyChannel<Async<ResponsePacket>>
    | Ping
    | GetData of string * AsyncReplyChannel<Async<ResponsePacket>>

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

type ZookeeperClient() =
    let deserialize f response = f(response.Header, response.Content)
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
        let pendingRequests = new System.Collections.Concurrent.ConcurrentQueue<RequestPacket>()
        let watchers = new System.Collections.Concurrent.ConcurrentDictionary<string, Watcher>()
        
        let rec receive (stream : Stream) =
            let responseSize = stream |> BigEndianReader.ReadInt32
            let ms = new MemoryStream(stream |> BigEndianReader.Read responseSize)
            let header = ms |> ReplyHeader.Deserialize
            let (success, request) = pendingRequests.TryDequeue()
            if not success && header.Xid <> XidConstants.Ping && header.Xid <> XidConstants.Notification then
                raise (new IOException(sprintf "Nothing in queue, but got xid %i" header.Xid))
            if header.Zxid > 0L then Interlocked.Increment(lastZxid) |> ignore
            if header.Error <> 0 then raise (new IOException(sprintf "Got error code %i" header.Error))
            match header.Xid with
            | XidConstants.Ping -> LogConfiguration.Logger.Trace.Invoke("Got ping response")
            | XidConstants.Notification ->
                let response = WatcherEvent.Deserialize(header, ms)
                let success, watcher = watchers.TryGetValue(response.Path)
                if success then
                    inbox.PostAndReply(fun reply -> GetChildrenWithWatcher(response.Path, watcher, reply)) |> Async.Ignore |> Async.Start
                    watcher.Callback()
                else LogConfiguration.Logger.Warning.Invoke(sprintf "Got notification '%A', but cannot find a matching watcher" response.Path)
            | x when x = request.Xid ->
                LogConfiguration.Logger.Trace.Invoke(sprintf "Received with xid %i" header.Xid)
                { Header = header; Content = ms } |> request.ResponseReceived
            | _ ->
                raise (new IOException(sprintf "Xid out of order. Got %i expected %i" header.Xid request.Xid))
            receive stream
        
        let rec loop state = async {
            let xid = state.LastXid + 1
            let! msg = inbox.Receive()
            match msg with
            | Connect (host, port, sessionTimeout, reply) ->
                try
                    let client = state.TcpClient |> TcpClient.connectTo host port
                    let request = new ConnectRequest(0, lastZxid.Value, sessionTimeout, int64 0, Array.zeroCreate(16))
                    let response =
                        client
                        |> TcpClient.write (request.Serialize(xid))
                        |> TcpClient.read ConnectResponse.Deserialize
                    let receiver = async { do receive client.Stream.Value }
                    receiver |> Async.Start
                    reply.Reply(Success())
                    let pinger = inbox |> createPinger sessionTimeout
                    return! loop { state with TcpClient = client; SessionId = response.SessionId; Password = response.Password; SessionTimeout = sessionTimeout; Receiver = Some receiver; Pinger = Some pinger }
                with
                | e -> reply.Reply(Error(e))
            | GetChildrenWithWatcher (path, watcher, reply) ->
                let request = new GetChildrenRequest(path, true)
                let requestPacket = new RequestPacket(xid)
                pendingRequests.Enqueue(requestPacket)
                watchers.TryAdd(path, watcher) |> ignore
                state.TcpClient |> TcpClient.write (request.Serialize(xid)) |> ignore
                reply.Reply(requestPacket.GetResponseAsync(state.SessionTimeout))
                return! loop { state with LastXid = xid }
            | GetChildren (path, reply) ->
                let request = new GetChildrenRequest(path, false)
                let requestPacket = new RequestPacket(xid)
                pendingRequests.Enqueue(requestPacket)
                state.TcpClient |> TcpClient.write (request.Serialize(xid)) |> ignore
                reply.Reply(requestPacket.GetResponseAsync(state.SessionTimeout))
                return! loop { state with LastXid = xid }
            | Ping ->
                state.TcpClient |> TcpClient.write ((new PingRequest()).Serialize(XidConstants.Ping)) |> ignore
                return! loop { state with LastXid = xid }
            | GetData (path, reply) ->
                let request = new GetDataRequest(path)
                let requestPacket = new RequestPacket(xid)
                pendingRequests.Enqueue(requestPacket)
                state.TcpClient |> TcpClient.write (request.Serialize(xid)) |> ignore
                reply.Reply(requestPacket.GetResponseAsync(state.SessionTimeout))
                return! loop { state with LastXid = xid }
        }
        loop { TcpClient = TcpClient.create(); SessionId = 0L; Password = Array.empty; LastXid = 0; SessionTimeout = 0; Receiver = None; Pinger = None })

    do
        agent.Error.Add(fun x -> LogConfiguration.Logger.Fatal.Invoke("Got exception in zookeeper agent", x))

    member __.Connect(host, port, sessionTimeout) =
        let result = agent.PostAndReply(fun reply -> Connect(host, port, sessionTimeout, reply))
        match result with
        | Error x -> raise x
        | Success _ -> ()
    member __.GetChildren(path, watcherCallback) =
        let watcher = { Path = path; Callback = watcherCallback }
        agent.PostAndReply(fun reply -> GetChildrenWithWatcher(path, watcher, reply))
        |> Async.RunSynchronously
        |> deserialize GetChildrenResponse.Deserialize
    member __.GetChildren(path) =
        agent.PostAndReply(fun reply -> GetChildren(path, reply))
        |> Async.RunSynchronously
        |> deserialize GetChildrenResponse.Deserialize
    member __.GetData(path) =
        agent.PostAndReply(fun reply -> GetData(path, reply))
        |> Async.RunSynchronously
        |> deserialize GetDataResponse.Deserialize
