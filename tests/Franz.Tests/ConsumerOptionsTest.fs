namespace Franz.Tests

open Franz.HighLevel
open Xunit
open Swensen.Unquote

type ConsumerOptionsTest() =

    [<Fact>]
    member __.``default offsetstorage value is Zookeeper`` () =
        let options = new ConsumerOptions()
        test <@ options.OffsetStorage = OffsetStorage.Zookeeper @>

    [<Fact>]
    member __.``setting a offsetstorage value different than the default value is possible`` () =
        let options = new ConsumerOptions()
        options.OffsetStorage <- OffsetStorage.DualCommit
        test <@ options.OffsetStorage = OffsetStorage.DualCommit @>
