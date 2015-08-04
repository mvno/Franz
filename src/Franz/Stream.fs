namespace Franz.Stream

open System
open System.IO
open System.Text
open Franz.Internal

[<AbstractClass; Sealed>]
type BigEndianWriter() =
    static let kafkaNullSize = -1

    static member ConvertToBigEndian isLittleEndian x =
        if isLittleEndian then x |> Array.rev else x

    static member Write (stream : Stream) bytes =
        stream.Write(bytes, 0, bytes.Length)

    static member WriteInt8 (x : int8) stream =
        [| x |> byte |] |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian |> BigEndianWriter.Write stream

    static member WriteInt16 (x : Int16) stream =
        x |> BitConverter.GetBytes |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian |> BigEndianWriter.Write stream

    static member WriteInt32 (x : Int32) stream =
        x |> BitConverter.GetBytes |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian |> BigEndianWriter.Write stream

    static member WriteInt64 (x : Int64) stream =
        x |> BitConverter.GetBytes |> BigEndianWriter.ConvertToBigEndian BitConverter.IsLittleEndian |> BigEndianWriter.Write stream

    static member WriteString (x : string) stream =
        if x = null then
            stream |> BigEndianWriter.WriteInt16 (Convert.ToInt16(kafkaNullSize))
        else
            stream |> BigEndianWriter.WriteInt16 (Convert.ToInt16(x.Length))
            x |> Encoding.UTF8.GetBytes |> BigEndianWriter.Write stream

    static member WriteBytes (x : byte array) stream =
        if x = null then
            stream |> BigEndianWriter.WriteInt32 kafkaNullSize
        else
            stream |> BigEndianWriter.WriteInt32 x.Length
            x |> BigEndianWriter.Write stream

[<AbstractClass; Sealed>]
type BigEndianReader() =
    static let kafkaNullSize = -1

    static member ReadEndianAware isLittleEndian bytes =
        if isLittleEndian then bytes |> Array.rev
        else bytes

    static member Read size (stream : Stream) =
        let buffer = Array.zeroCreate(size)
        let rec innerRead offset bytesLeft =
            let bytesRead = stream.Read(buffer, offset, bytesLeft)
            if bytesRead <> bytesLeft then
                innerRead (offset + bytesRead) (bytesLeft - bytesRead)
        innerRead 0 size
        buffer

    static member ReadInt8 stream =
        let bytes = stream |> BigEndianReader.Read 1 |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        int8 bytes.[0]

    static member ReadInt16 stream =
        let bytes = stream |> BigEndianReader.Read 2 |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt16(bytes, 0)

    static member ReadInt32 stream =
        let bytes = stream |> BigEndianReader.Read 4 |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt32(bytes, 0)

    static member ReadInt64 stream =
        let bytes = stream |> BigEndianReader.Read 8 |> BigEndianReader.ReadEndianAware BitConverter.IsLittleEndian
        BitConverter.ToInt64(bytes, 0)

    static member ReadString stream =
        let size = stream |> BigEndianReader.ReadInt16 |> int
        if size = kafkaNullSize then null
        else stream |> BigEndianReader.Read size |> Encoding.UTF8.GetString

    static member ReadBytes stream =
        let size = stream |> BigEndianReader.ReadInt32
        if size = kafkaNullSize then null
        else stream |> BigEndianReader.Read size
