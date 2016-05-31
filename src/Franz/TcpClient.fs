namespace Franz.Internal

[<RequireQualifiedAccess>]
module TcpClient =
    open System.IO
    open System.Net.Sockets

    type TcpState = | NotConnected | Connected
    type T = { Client : TcpClient; State : TcpState; Stream : Stream option; Host : string; Port : int }
    let create() = { Client = new TcpClient(); State = NotConnected; Stream = None; Host = null; Port = 0 }
    let isConnected client = client.State = Connected
    let isDisconnected client = isConnected client |> not
    let connectTo (host : string) (port : int) (client : T) =
        if client |> isConnected then invalidOp "Already connected"
        client.Client.Connect(host, port)
        { client with Stream = Some (client.Client.GetStream() :> Stream); State = Connected; Host = host; Port = port }
    let write buffer client =
        if client |> isDisconnected then invalidOp "Not connected"
        client.Stream.Value.Write(buffer, 0, buffer.Length)
        client
    let readToBuffer numberOfBytesToRead client =
        if client |> isDisconnected then invalidOp "Not connected"
        if numberOfBytesToRead < 1 then invalidOp "Must read more than zero bytes"
        let buffer = Array.zeroCreate(numberOfBytesToRead)
        let bytesRead = client.Stream.Value.Read(buffer, 0, buffer.Length)
        if bytesRead = 0 then invalidOp "Connection lost"
        buffer
    let read f client =
        if client |> isDisconnected then invalidOp "Not connected"
        client.Stream.Value |> f
    let reconnect client =
        let newClient = { client with Client = new TcpClient(); Stream = None; State = NotConnected }
        newClient |> connectTo client.Host client.Port