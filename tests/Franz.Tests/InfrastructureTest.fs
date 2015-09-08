namespace Franz.Tests

open Franz.Internal
open Xunit
open Swensen.Unquote

type InternalTests() =
    [<Fact>]
    member __.``roundRobin on single element list returns first element when the last index is 0`` () =
        let list = [1]
        let lastIndex = 0;
        
        let (_, element) = Seq.roundRobin lastIndex list
        
        test <@ element = (list |> Seq.head) @>

    [<Fact>]
    member __.``roundRobin on single element list returns first element when the last index is greater than 0`` () =
        let list = [1]
        let lastIndex = 10;
        
        let (_, element) = Seq.roundRobin lastIndex list
        
        test <@ element = (list |> Seq.head) @>

    [<Fact>]
    member __.``roundRobin on single element list returns new position as 0`` () =
        let list = [1]
        let lastIndex = 10;
        
        let (newPosition, _) = Seq.roundRobin lastIndex list
        
        test <@ newPosition = 0 @>

    [<Fact>]
    member __.``roundRobin returns second element when last position is 0`` () =
        let list = [1; 2]
        let lastPostion = 0
        
        test
            <@
                let (_, element) = Seq.roundRobin lastPostion list
                element = Seq.last list
            @>

    [<Fact>]
    member __.``roundRobin returns next position as last position plus one`` () =
        let list = [1; 2; 3]
        let lastPostion = 1
        
        test
            <@
                let (newPosition, _) = Seq.roundRobin lastPostion list
                newPosition = lastPostion + 1
            @>
