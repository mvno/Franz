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
    let convertToInt64 array =
        if BitConverter.IsLittleEndian then
            array |> Array.Reverse
        BitConverter.ToInt64(array, 0)
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
                size = 0
                && apiKey = expectedApiKey
                && apiVersion = requestBase.ApiVersion
                && correlationId = requestBase.CorrelationId - 1 // Subtract one as this value is incremented on each call
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
                && correlationId = requestBase.CorrelationId - 1 // Subtract one as this value is incremented on each call
                && clientIdLength = (int16 5)
                && clientIdValue = "Franz"
                && content = requestBase.Content
            @>

type MessageSetTest() =
    [<Fact>]
    member __.``creating a message set sets the offset correctly`` () =
        let offset = int64 100
        
        let messageSet = MessageSet.Create(offset, int8 0, [||], [||])
        
        test <@ messageSet.Offset = offset @>

    [<Fact>]
    member __.``creating a message set sets the size correctly`` () =
        let expectedMessageSize = 14

        let messageSet = MessageSet.Create(int64 0, int8 0, [||], [||])
        
        test <@ messageSet.MessageSize = expectedMessageSize @>

    [<Fact>]
    member __.``creating a message set sets the message correctly`` () =
        let attributes = int8 1
        let key = Encoding.UTF8.GetBytes("Key")
        let value = Encoding.UTF8.GetBytes("Value")
        
        let messageSet = MessageSet.Create(int64 0, attributes, key, value)
        
        let message = messageSet.Message
        let expectedMagicByte = int8 0
        test
            <@
                message.Attributes = attributes
                && message.MagicByte = expectedMagicByte
                && message.Key |> Seq.forall2 (fun x y -> x = y) key
                && message.Value |> Seq.forall2 (fun x y -> x = y) value
            @>

    [<Fact>]
    member __.``message set is serialized correctly`` () =
        let stream = new MemoryStream()
        let offset = int64 100
        let messageSize = 120
        let magicByte = int8 0
        let attributes = int8 0
        let key = Encoding.UTF8.GetBytes("Key")
        let value = Encoding.UTF8.GetBytes("Value")
        let crc = 0
        let messageSet = new MessageSet(offset, messageSize, { Crc = crc; MagicByte = magicByte; Attributes = attributes; Key = key; Value = value })
        
        messageSet.Serialize(stream)
        
        stream.Position <- int64 0
        let buffer = Array.zeroCreate(stream.Length |> int32)
        stream.Read(buffer, 0, stream.Length |> int32) |> ignore
        let actualOffset = buffer |> Array.take(8) |> Helpers.convertToInt64
        let actualMessageSize = buffer |> Array.skip(8) |> Array.take(4) |> Helpers.convertToInt32
        let actualCrc = buffer |> Array.skip(12) |> Array.take(4) |> Helpers.convertToInt32
        let actualMagicByte = buffer |> Array.skip(16) |> Array.take(1) |> Array.head
        let actualAttributes = buffer |> Array.skip(17) |> Array.take(1) |> Array.head
        let actualKeyLength = buffer |> Array.skip(18) |> Array.take(4) |> Helpers.convertToInt32
        let actualKeyValue = buffer |> Array.skip(22) |> Array.take(actualKeyLength)
        let actualValueLength = buffer |> Array.skip(22 + actualKeyLength) |> Array.take(4) |> Helpers.convertToInt32
        let actualValueValue = buffer |> Array.skip(22 + actualKeyLength + 4) |> Array.take(actualValueLength)
        let expectedMagicByte = byte magicByte
        let expectedAttributes = byte attributes
        test
            <@
                actualOffset = offset
                && actualMessageSize = messageSize
                && actualCrc = crc
                && actualMagicByte = expectedMagicByte
                && actualAttributes = expectedAttributes
                && actualKeyValue = key
                && actualValueValue = value
            @>
