namespace Franz.Tests

open Snappy
open Franz
open Franz.Stream
open Franz.Compression
open System.Text
open Swensen.Unquote
open Xunit
open System.IO
open System.IO.Compression

module Helper =
    let toBytes (messageSets : MessageSet seq) =
       use stream = new MemoryStream()
       messageSets |> Seq.iter (fun x -> x.Serialize(stream))
       stream.Seek(0L, SeekOrigin.Begin) |> ignore
       let buffer = Array.zeroCreate(int stream.Length)
       stream.Read(buffer, 0, int stream.Length) |> ignore
       buffer
    let compressWithGzip (buffer : byte array) =
        use stream = new MemoryStream()
        use gzipStream = new GZipStream(stream, CompressionLevel.Fastest, false)
        gzipStream.Write(buffer, 0, buffer.Length)
        gzipStream.Flush()
        gzipStream.Close()
        stream.ToArray()
    let compressWithSnappy (buffer : byte array) =
        SnappyCodec.Compress(buffer)
    let compressWithSnappyXerialFormat (buffer : byte array) =
        let writeDummyHeader (stream : Stream) =
            stream.Write(Array.zeroCreate(16), 0, 16)
        let writeBlockSize size stream =
            stream |> BigEndianWriter.WriteInt32 size
        use stream = new MemoryStream()
        stream |> writeDummyHeader
        let compressedBuffer = SnappyCodec.Compress(buffer)
        stream |> writeBlockSize compressedBuffer.Length
        stream.Write(compressedBuffer, 0, compressedBuffer.Length)
        stream.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate(int stream.Length)
        stream.Read(buffer, 0, int stream.Length) |> ignore
        buffer

type GzipCompressionTest() =
    [<Fact>]
    member __.``encoding messagesets returns a single messageset`` () =
        let messageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")) ]
        
        let compressedMessageset = GzipCompression.Encode(messageSets)

        test <@ compressedMessageset |> Seq.length = 1 @>

    [<Fact>]
    member __.``encoding a messageset returns a messageset containing the encoded messageset as the value`` () =
        let messageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")) ]
        let serializedMessageSets = messageSets |> Helper.toBytes
        let compressedBytes = serializedMessageSets |> Helper.compressWithGzip
        
        let compressedMessageSet = GzipCompression.Encode(messageSets) |> Seq.exactlyOne

        test <@ compressedMessageSet.Message.Value = compressedBytes @>

    [<Fact>]
    member __.``encoding a messageset returns a messagetset with the gzip compression flag`` () =
        let messageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")) ]
        
        let compressedMessageSet = GzipCompression.Encode(messageSets) |> Seq.exactlyOne

        test <@ compressedMessageSet.Message.Attributes = 1y @>

    [<Fact>]
    member __.``decoding a messageset not compressed by gzip throws a exception`` () =
        let messageSet = MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text"))

        raises<exn> <@ GzipCompression.Decode(messageSet) @>

    [<Fact>]
    member __.``decoding a messageset compressed by gzip returns the decompressed messagesets`` () =
        let compressedMessageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text2")) ]
        let compressedData =
            compressedMessageSets
            |> Helper.toBytes
            |> Helper.compressWithGzip
        let messageSet = MessageSet.Create(0L, 1y, null, compressedData)

        let decompressedMessageSets = GzipCompression.Decode(messageSet)

        test
            <@
                decompressedMessageSets |> Seq.exists (fun x -> Encoding.UTF8.GetString(x.Message.Value) = "Compressed text")
                && decompressedMessageSets |> Seq.exists (fun x -> Encoding.UTF8.GetString(x.Message.Value) = "Compressed text2")
            @>

type SnappyCompressionTest() =
    [<Fact>]
    member __.``encoding messagesets returns a single messageset`` () =
        let messageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")) ]
        
        let compressedMessageset = SnappyCompression.Encode(messageSets)

        test <@ compressedMessageset |> Seq.length = 1 @>

    [<Fact>]
    member __.``encoding a messageset returns a messageset containing the encoded messageset as the value`` () =
        let messageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")) ]
        let serializedMessageSets = messageSets |> Helper.toBytes
        let compressedBytes = serializedMessageSets |> Helper.compressWithSnappy
        
        let compressedMessageSet = SnappyCompression.Encode(messageSets) |> Seq.exactlyOne

        test <@ compressedMessageSet.Message.Value = compressedBytes @>

    [<Fact>]
    member __.``decoding a messageset not compressed by snappy throws a exception`` () =
        let messageSet = MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text"))

        raises<exn> <@ GzipCompression.Decode(messageSet) @>
        
    [<Fact>]
    member __.``decoding a messageset compressed by xerial snappy format returns the decompressed messagesets`` () =
        let compressedMessageSets = [ MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text")); MessageSet.Create(100L, 0y, null, Encoding.UTF8.GetBytes("Compressed text2")) ]
        let compressedData =
            compressedMessageSets
            |> Helper.toBytes
            |> Helper.compressWithSnappyXerialFormat
        let messageSet = MessageSet.Create(0L, 2y, null, compressedData)

        let decompressedMessageSets = SnappyCompression.Decode(messageSet)

        test
            <@
                decompressedMessageSets |> Seq.exists (fun x -> Encoding.UTF8.GetString(x.Message.Value) = "Compressed text")
                && decompressedMessageSets |> Seq.exists (fun x -> Encoding.UTF8.GetString(x.Message.Value) = "Compressed text2")
            @>
