namespace Franz.Tests

open Franz
open Xunit
open Swensen.Unquote
open System.IO
open System
open System.Text

module Helpers =
    let convertToInt32 array =
        if BitConverter.IsLittleEndian then
            array |> Array.Reverse
        BitConverter.ToInt32(array, 0)
    let convertToInt16 array =
        if BitConverter.IsLittleEndian then
            array |> Array.Reverse
        BitConverter.ToInt16(array, 0)
    let convertToString array =
        Encoding.UTF8.GetString(array)

type RequestBaseImpl() =
    inherit Request<string>()
    member __.Content = byte 10
    override __.ApiKey = ApiKey.FetchRequest
    override __.DeserializeResponse(stream) =
        ""
    override self.SerializeMessage(stream) =
        stream.WriteByte(self.Content)

type RequestBaseTest() =
    let requestBase = new RequestBaseImpl()
    
    [<Fact>]
    member __.``default API version is 0`` () =
        test <@ requestBase.ApiVersion = int16 0 @>

    [<Fact>]
    member __.``default client id is Franz`` () =
        test <@ requestBase.ClientId = "Franz" @>

    [<Fact>]
    member __.``header is serialize correctly but without actual size`` () =
        let stream = new MemoryStream()
        requestBase.SerializeHeader(stream)
        stream.Position <- int64 0
        let buffer = Array.zeroCreate(stream.Length |> int)
        stream.Read(buffer, 0, stream.Length |> int) |> ignore
        let size = buffer |> Array.take (4) |> Helpers.convertToInt32
        let apiKey = buffer |> Array.skip(4) |> Array.take(2) |> Helpers.convertToInt16
        let apiVersion = buffer |> Array.skip(6) |> Array.take(2) |> Helpers.convertToInt16
        let correlationId = buffer |> Array.skip(8) |> Array.take(4) |> Helpers.convertToInt32
        let clientIdLength = buffer |> Array.skip(12) |> Array.take(2) |> Helpers.convertToInt16
        let clientIdValue = buffer |> Array.skip(14) |> Array.take(clientIdLength |> int) |> Helpers.convertToString
        let expectedApiKey = requestBase.ApiKey |> int |> int16
        test
            <@
                size = 4
                && apiKey = expectedApiKey
                && apiVersion = requestBase.ApiVersion
                && correlationId = requestBase.CorrelationId
                && clientIdLength = (int16 5)
                && clientIdValue = "Franz"
            @>

    [<Fact>]
    member __.``size is written as the stream lenght minus 4 bytes`` () =
        let streamSize = 10
        let intSize = 4
        let expectedSize = streamSize - intSize
        let stream = new MemoryStream(Array.zeroCreate(streamSize))
        requestBase.WriteSize(stream) |> ignore
        stream.Position <- int64 0
        let buffer = Array.zeroCreate(4)
        stream.Read(buffer, 0, intSize) |> ignore
        let actualSize = buffer |> Helpers.convertToInt32
        test <@ actualSize = expectedSize @>

    [<Fact>]
    member __.``serialize serializes the header and message correctly including the size`` () =
        let intSize = 4
        let stream = new MemoryStream()
        requestBase.Serialize(stream)
        stream.Position <- int64 0
        let buffer = Array.zeroCreate(stream.Length |> int)
        stream.Read(buffer, 0, stream.Length |> int) |> ignore
        let size = buffer |> Array.take (4) |> Helpers.convertToInt32
        let apiKey = buffer |> Array.skip(4) |> Array.take(2) |> Helpers.convertToInt16
        let apiVersion = buffer |> Array.skip(6) |> Array.take(2) |> Helpers.convertToInt16
        let correlationId = buffer |> Array.skip(8) |> Array.take(4) |> Helpers.convertToInt32
        let clientIdLength = buffer |> Array.skip(12) |> Array.take(2) |> Helpers.convertToInt16
        let clientIdValue = buffer |> Array.skip(14) |> Array.take(clientIdLength |> int) |> Helpers.convertToString
        let expectedApiKey = requestBase.ApiKey |> int |> int16
        let expectedSize = (stream.Length |> int) - intSize
        let content = buffer |> Array.skip(14 + (int clientIdLength)) |> Array.take(1) |> Array.head
        test
            <@
                size = expectedSize
                && apiKey = expectedApiKey
                && apiVersion = requestBase.ApiVersion
                && correlationId = requestBase.CorrelationId
                && clientIdLength = (int16 5)
                && clientIdValue = "Franz"
                && content = requestBase.Content
            @>
