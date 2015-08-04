namespace Franz.Internal

[<AutoOpen>]
module Crc32 =
    open System

    let crcTable = 
        let inline nextValue acc =
            if 0u <> (acc &&& 1u) then 0xedb88320u ^^^ (acc >>> 1) else acc >>> 1
        let rec iter k acc =
            if k = 0 then acc else iter (k-1) (nextValue acc)
        [| 0u .. 255u |] |> Array.map (iter 8)


    let crc32 (buffer : byte array) =
        let inline update acc (byte : byte) =
            crcTable.[int32 ((acc ^^^ (uint32 byte)) &&& 0xffu)] ^^^ (acc >>> 8)
        0xFFffFFffu ^^^ Seq.fold update 0xFFffFFffu buffer |> int32
