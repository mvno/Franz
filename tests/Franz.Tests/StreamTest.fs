namespace Franz.Tests

open Franz.Stream
open Xunit
open System
open System.IO
open System.Text
open Swensen.Unquote
open FsCheck

type BigEndianWriterTest() =
    let mutable stream = new MemoryStream()
    let seekToStart () =
        stream.Seek(int64 0, SeekOrigin.Begin) |> ignore
    let resetStream () =
        stream.Dispose()
        stream <- new MemoryStream()

    interface IDisposable with
        member __.Dispose() =
            stream.Dispose()

    [<Fact>]
    member __.``convert to big endian reverse byte array if platform is little endian2`` () =
        let check (array : byte array) = BigEndianWriter.ConvertToBigEndian true array = (array |> Array.rev)
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``convert to big endian not reverse byte array if platform is big endian`` () =
        let check (array : byte array) = BigEndianWriter.ConvertToBigEndian false array = array
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``write writes the byte array to the stream`` () =
        let check (array : byte array) =
            BigEndianWriter.Write stream array
            seekToStart()
            let bytes = Array.zeroCreate(array.Length)
            stream.Read(bytes, 0, array.Length) |> ignore
            resetStream()
            bytes = array
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeInt16 writes an int16 to the stream`` () =
        let check (value : int16) =
            BigEndianWriter.WriteInt16 value stream
            seekToStart()
            let bytes = Array.zeroCreate(2)
            stream.Read(bytes, 0, bytes.Length) |> ignore
            resetStream()
            if BitConverter.IsLittleEndian then bytes |> Array.Reverse
            BitConverter.ToInt16(bytes, 0) = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeInt32 writes an int32 to the stream`` () =
        let check (value : int) =
            BigEndianWriter.WriteInt32 value stream
            seekToStart()
            let bytes = Array.zeroCreate(4)
            stream.Read(bytes, 0, bytes.Length) |> ignore
            resetStream()
            if BitConverter.IsLittleEndian then bytes |> Array.Reverse
            BitConverter.ToInt32(bytes, 0) = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeString writes a string writes the length of the stream first in int16`` () =
        let check (value : string) =
            let innerCheck() =
                BigEndianWriter.WriteString value stream
                seekToStart()
                let bytes = Array.zeroCreate(2)
                stream.Read(bytes, 0, bytes.Length) |> ignore
                resetStream()
                if BitConverter.IsLittleEndian then bytes |> Array.Reverse
                BitConverter.ToInt16(bytes, 0) = int16 value.Length
            value <> null ==> lazy (innerCheck())
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeString writes a string writes the string after the length`` () =
        let check (value : string) =
            let innerCheck() =
                BigEndianWriter.WriteString value stream
                seekToStart()
                stream.Seek(int64 2, SeekOrigin.Current) |> ignore
                let bytes = Array.zeroCreate(value.Length)
                stream.Read(bytes, 0, bytes.Length) |> ignore
                resetStream()
                Encoding.UTF8.GetString(bytes) = value
            value <> null ==> lazy (innerCheck()) 
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeString writes a string that is null with length -1`` () =
        BigEndianWriter.WriteString null stream

        seekToStart()
        let bytes = Array.zeroCreate(2)
        stream.Read(bytes, 0, 2) |> ignore
        if BitConverter.IsLittleEndian then bytes |> Array.Reverse
        test <@ BitConverter.ToInt16(bytes, 0) = Convert.ToInt16(-1) @>

    [<Fact>]
    member __.``writeBytes writes the length of the byte array first in int32`` () =
        let check (value : byte array) =
            let innerCheck() =
                BigEndianWriter.WriteBytes value stream
                seekToStart()
                let bytes = Array.zeroCreate(4)
                stream.Read(bytes, 0, bytes.Length) |> ignore
                resetStream()
                if BitConverter.IsLittleEndian then bytes |> Array.Reverse
                BitConverter.ToInt32(bytes, 0) = int32 value.Length
            value <> null ==> lazy (innerCheck())
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``writeBytes writes a byte array that is null with length -1`` () =
        BigEndianWriter.WriteBytes null stream

        seekToStart()
        let bytes = Array.zeroCreate(4)
        stream.Read(bytes, 0, 4) |> ignore
        if BitConverter.IsLittleEndian then bytes |> Array.Reverse
        test <@ BitConverter.ToInt32(bytes, 0) = Convert.ToInt32(-1) @>

    [<Fact>]
    member __.``writeBytes writes the byte array after the length`` () =
        let check (value : byte array) =
            let innerCheck() =
                BigEndianWriter.WriteBytes value stream
                seekToStart()
                stream.Seek(int64 4, SeekOrigin.Current) |> ignore
                let bytes = Array.zeroCreate(value.Length)
                stream.Read(bytes, 0, bytes.Length) |> ignore
                resetStream()
                bytes = value
            value <> null ==> lazy (innerCheck()) 
        Check.QuickThrowOnFailure check

type BigEndianReader() =
    let mutable stream = new MemoryStream()
    let mutable streamWriter = new BinaryWriter(stream)
    let seekToStart () =
        stream.Seek(int64 0, SeekOrigin.Begin) |> ignore
    let resetStream () =
        stream.Dispose()
        streamWriter.Dispose()
        stream <- new MemoryStream()
        streamWriter <- new BinaryWriter(stream)

    interface IDisposable with
        member __.Dispose() =
            streamWriter.Dispose()

    [<Fact>]
    member __.``readEndianAware reverse byte array is platform is little endian`` () =
        let check (value : byte array) =
            let result = BigEndianReader.ReadEndianAware true value
            result = (value |> Array.rev)
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``readEndianAware do not reverse byte array is platform is big endian`` () =
        let check (value : byte array) =
            let result = BigEndianReader.ReadEndianAware false value
            result = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``read reads the number of bytes specified`` () =
        let check (value : byte array) =
            value |> Seq.iter (fun x -> streamWriter.Write(x))
            seekToStart()
            let result = BigEndianReader.Read value.Length stream
            resetStream()
            result = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``readInt16 reads an int16 from the stream`` () =
        let check (value : int16) =
            let bytes = BitConverter.GetBytes(value)
            if BitConverter.IsLittleEndian then bytes |> Array.Reverse
            stream.Write(bytes, 0, bytes.Length)
            seekToStart()
            let result = BigEndianReader.ReadInt16 stream
            resetStream()
            result = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``readInt32 reads an int32 from the stream`` () =
        let check (value : int32) =
            let bytes = BitConverter.GetBytes(value)
            if BitConverter.IsLittleEndian then bytes |> Array.Reverse
            stream.Write(bytes, 0, bytes.Length)
            seekToStart()
            let result = BigEndianReader.ReadInt32 stream
            resetStream()
            result = value
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``readString reads a string from the stream`` () =
        let check (value : string) =
            let innerCheck() =
                let bytes = BitConverter.GetBytes(int16 value.Length)
                if BitConverter.IsLittleEndian then bytes |> Array.Reverse
                stream.Write(bytes, 0, bytes.Length)
                let bytes = Encoding.UTF8.GetBytes(value)
                stream.Write(bytes, 0, bytes.Length)
                seekToStart()
                let result = BigEndianReader.ReadString stream
                resetStream()
                result = value
            value <> null ==> lazy (innerCheck())
        Check.QuickThrowOnFailure check

    [<Fact>]
    member __.``readBytes reads a string from the stream`` () =
        let check (value : byte array) =
            let innerCheck () =
                let bytes = BitConverter.GetBytes(int32 value.Length)
                if BitConverter.IsLittleEndian then bytes |> Array.Reverse
                stream.Write(bytes, 0, bytes.Length)
                let bytes = value
                stream.Write(bytes, 0, bytes.Length)
                seekToStart()
                let result = BigEndianReader.ReadBytes stream
                resetStream()
                result = value
            value <> null ==> lazy(innerCheck())
        Check.QuickThrowOnFailure check
