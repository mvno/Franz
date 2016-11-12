namespace Franz.Stream

open System
open System.IO
open System.Text

type UnderlyingConnectionClosedException() = 
    inherit Exception()
    override e.Message = "Could not read any data from stream."

/// Writes to a stream conforming to the Kafka protocol
[<AbstractClass; Sealed>]
type BigEndianWriter() = 
    
    /// The size unused to indicate NULL in the protocol
    static let kafkaNullSize = -1
    
    /// The size unused to indicate NULL in the protocol
    static let zkNullSize = -1
    
    /// Convert an array to big endian if needed
    static member ConvertToBigEndian isLittleEndian x = 
        if isLittleEndian then x |> Array.rev
        else x
    
    /// Write an array of bytes as passed in to the function
    static member Write (stream : Stream) bytes = stream.Write(bytes, 0, bytes.Length)
    
    /// Write an int8 value to the stream
    static member WriteInt8 (x : int8) stream = 
        [| x |> byte |]
        |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian
        |> BigEndianWriter.Write stream
    
    /// Write an int16 value to the stream
    static member WriteInt16 (x : Int16) stream = 
        x
        |> BitConverter.GetBytes
        |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian
        |> BigEndianWriter.Write stream
    
    /// Write an int32 value to the stream
    static member WriteInt32 (x : Int32) stream = 
        x
        |> BitConverter.GetBytes
        |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian
        |> BigEndianWriter.Write stream
    
    /// Write an int64 value to the stream
    static member WriteInt64 (x : Int64) stream = 
        x
        |> BitConverter.GetBytes
        |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian
        |> BigEndianWriter.Write stream
    
    /// Write an string value to the stream, prefixed by a int16 specifying the lenght of the string
    static member WriteString (x : string) stream = 
        if x |> isNull then stream |> BigEndianWriter.WriteInt16(Convert.ToInt16(kafkaNullSize))
        else 
            stream |> BigEndianWriter.WriteInt16(Convert.ToInt16(x.Length))
            x
            |> Encoding.UTF8.GetBytes
            |> BigEndianWriter.Write stream
    
    static member WriteZKString (x : string) stream = 
        if x |> isNull then stream |> BigEndianWriter.WriteInt32 zkNullSize
        else 
            stream |> BigEndianWriter.WriteInt32 x.Length
            x
            |> Encoding.UTF8.GetBytes
            |> BigEndianWriter.Write stream
    
    /// Write a byte array to the stream, with a prefixed int32 indicating the size of the array
    static member WriteBytes (x : byte array) stream = 
        if x |> isNull then stream |> BigEndianWriter.WriteInt32 kafkaNullSize
        else 
            stream |> BigEndianWriter.WriteInt32 x.Length
            x |> BigEndianWriter.Write stream

/// Reads from a stream conforming to the Kafka protocol
[<AbstractClass; Sealed>]
type BigEndianReader() = 
    
    /// The size unused to indicate NULL in the protocol
    static let kafkaNullSize = -1
    
    /// The size unused to indicate NULL in the protocol
    static let zkNullSize = -1
    
    static let rec readLoop offset bytesLeft (stream : Stream) buffer = 
        let bytesRead = stream.Read(buffer, offset, bytesLeft)
        if bytesRead <> bytesLeft then 
            if bytesRead = 0 then raise (UnderlyingConnectionClosedException())
            readLoop (offset + bytesRead) (bytesLeft - bytesRead) stream buffer
    
    /// Convert an array to big endian if needed
    static member ReadEndianAware isLittleEndian bytes = 
        if isLittleEndian then bytes |> Array.rev
        else bytes
    
    /// Read a number of bytes from the stream
    static member Read size (stream : Stream) = 
        let buffer = Array.zeroCreate (size)
        readLoop 0 size stream buffer
        buffer
    
    /// Read an int8 from the stream
    static member ReadInt8 stream = 
        let bytes = 
            stream
            |> BigEndianReader.Read 1
            |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        int8 bytes.[0]
    
    /// Read an int16 from the stream
    static member ReadInt16 stream = 
        let bytes = 
            stream
            |> BigEndianReader.Read 2
            |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt16(bytes, 0)
    
    /// Read an int32 from the stream
    static member ReadInt32 stream = 
        let bytes = 
            stream
            |> BigEndianReader.Read 4
            |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt32(bytes, 0)
    
    /// Read an int64 from the stream
    static member ReadInt64 stream = 
        let bytes = 
            stream
            |> BigEndianReader.Read 8
            |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt64(bytes, 0)
    
    /// Read a string from the stream, using the prefixed int16 to determine the length of the string
    static member ReadString stream = 
        let size = 
            stream
            |> BigEndianReader.ReadInt16
            |> int
        if size = kafkaNullSize then null
        else 
            stream
            |> BigEndianReader.Read size
            |> Encoding.UTF8.GetString
    
    static member ReadZKString stream = 
        let size = 
            stream
            |> BigEndianReader.ReadInt32
            |> int
        if size = zkNullSize then null
        else 
            stream
            |> BigEndianReader.Read size
            |> Encoding.UTF8.GetString
    
    /// Read a byte array from the stream, using the prefixed int32 to determine the length of the array
    static member ReadBytes stream = 
        let size = stream |> BigEndianReader.ReadInt32
        if size = kafkaNullSize then null
        else stream |> BigEndianReader.Read size
