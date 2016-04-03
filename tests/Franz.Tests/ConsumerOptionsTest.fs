namespace Franz.Tests

open Franz.HighLevel
open Xunit

type ConsumerOptionsTest() =

    [<Fact>]
    member __.``default offsetstorage value is Zookeeper`` () =
        let options = new ConsumerOptions()
        Assert.Equal(options.OffsetStorage, OffsetStorage.Zookeeper)

    [<Fact>]
    member __.``setting a offsetstorage value different than the default value is possible`` () =
        let options = new ConsumerOptions()
        options.OffsetStorage <- OffsetStorage.DualCommit
        Assert.Equal(options.OffsetStorage, OffsetStorage.DualCommit)
