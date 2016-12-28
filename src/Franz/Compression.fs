namespace Franz.Compression

open Franz
open Franz.Stream
open Snappy
open System.IO.Compression
open System.IO

/// Handles Snappy compression and decompression
[<AbstractClass; Sealed>]
type SnappyCompression private () = 
    static let snappyCompressionFlag = 2y
    
    /// Encode message sets
    static member Encode(messageSets : MessageSet seq) = 
        use stream = new MemoryStream()
        messageSets |> Seq.iter (fun x -> x.Serialize(stream))
        stream.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate (int stream.Length)
        stream.Read(buffer, 0, int stream.Length) |> ignore
        [| MessageSet.Create(-1L, snappyCompressionFlag, null, SnappyCodec.Compress(buffer)) |]
    
    /// Decode a message set
    static member Decode(messageSet : MessageSet) = 
        let message = messageSet.Message
        if message.CompressionCodec <> CompressionCodec.Snappy then 
            invalidOp "This message is not compressed using Snappy"
        use stream = new MemoryStream(message.Value)
        use dest = new MemoryStream()
        
        // Currently Kafka uses a none standard Snappy format, the so called Xerial format.
        /// This format contains a specialized blocking format:
        ///
        /// |  Header  |  Block length  |  Block data  |  Block length  |  Block data  |
        /// | -------- | -------------- | ------------ | -------------- | ------------ |
        /// | 16 bytes |      int32     | snappy bytes |      int32     | snappy bytes |
        let handleXerialFormat() = 
            stream.Seek(16L, SeekOrigin.Current) |> ignore
            while stream.Position < stream.Length do
                let blockSize = stream |> BigEndianReader.ReadInt32
                let buffer = Array.zeroCreate (blockSize)
                stream.Read(buffer, 0, blockSize) |> ignore
                let decompressedBuffer = SnappyCodec.Uncompress(buffer)
                dest.Write(decompressedBuffer, 0, decompressedBuffer.Length)
        handleXerialFormat()
        dest.Seek(0L, SeekOrigin.Begin) |> ignore
        let buffer = Array.zeroCreate (int dest.Length)
        dest.Read(buffer, 0, int dest.Length) |> ignore
        buffer |> MessageSet.Deserialize

[<AbstractClass; Sealed>]
/// Handles GZip compression and decompression
type GzipCompression private () = 
    static let gzipCompressionFlag = 1y
    
    /// Decode a message set
    static member Decode(messageSet : MessageSet) = 
        let message = messageSet.Message
        if message.CompressionCodec <> CompressionCodec.Gzip then invalidOp "This message is not compressed using GZip"
        use source = new MemoryStream(message.Value)
        use destination = new MemoryStream()
        use gzipStream = new GZipStream(source, CompressionMode.Decompress, false)
        gzipStream.CopyTo(destination)
        gzipStream.Flush()
        gzipStream.Close()
        destination.ToArray() |> MessageSet.Deserialize
    
    /// Encode message sets
    static member Encode(messageSets : MessageSet seq) = 
        use stream = new MemoryStream()
        use gzipStream = new GZipStream(stream, CompressionLevel.Fastest, false)
        messageSets |> Seq.iter (fun x -> x.Serialize(gzipStream))
        gzipStream.Flush()
        gzipStream.Close()
        let buffer = stream.ToArray()
        [| MessageSet.Create(-1L, gzipCompressionFlag, null, buffer) |]

/// Helper methods related to compression
[<Sealed; AbstractClass>]
type Compression private () = 
    
    /// Decompress a single messageset
    static member DecompressMessageSet(messageSet : MessageSet) = 
        match messageSet.Message.CompressionCodec with
        | CompressionCodec.Gzip -> GzipCompression.Decode(messageSet)
        | CompressionCodec.Snappy -> SnappyCompression.Decode(messageSet)
        | CompressionCodec.None -> [ messageSet ]
        | x -> failwithf "Unknown compression codec %A" x
    
    /// Decompress multiple messagesets
    static member DecompressMessageSets(messageSets : MessageSet seq) = 
        messageSets
        |> Seq.map Compression.DecompressMessageSet
        |> Seq.concat
    
    /// Compress messages, using the specified compression codec
    static member CompressMessages(compressionCodec, messageSets : MessageSet array) = 
        match compressionCodec with
        | CompressionCodec.None -> messageSets
        | CompressionCodec.Gzip -> GzipCompression.Encode(messageSets)
        | CompressionCodec.Snappy -> SnappyCompression.Encode(messageSets)
        | x -> failwithf "Unsupported compression codec %A" x
