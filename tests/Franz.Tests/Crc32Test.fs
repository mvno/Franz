namespace Franz.Tests

open Xunit
open Franz.Internal
open System.Text
open Swensen.Unquote

type Crc32Test() =
    [<Fact>]
    member __.``CRC32 is calculated correctly`` () =
        let value = Encoding.UTF8.GetBytes("Test")
        
        test <@ Crc32.crc32(value) = 0x784dd132 @>
