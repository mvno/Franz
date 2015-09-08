namespace Franz.Tests

open Franz.Internal
open Xunit
open Swensen.Unquote

type InternalTests() =
    member __.``roundRobin on single element list returns first element when the last index is 0`` () =
        let list = [1]
        let lastIndex = 0;
        let (_, element) = Seq.roundRobin lastIndex list
        test <@ element = (list |> Seq.head) @>

    member __.``roundRobin on single element list returns first element when the last index is greater than 0`` () =
        let list = [1]
        let lastIndex = 10;
        let (_, element) = Seq.roundRobin lastIndex list
        test <@ element = (list |> Seq.head) @>

    member __.``roundRobin on single element list returns new position as 0`` () =
        let list = [1]
        let lastIndex = 10;
        let (newPosition, _) = Seq.roundRobin lastIndex list
        test <@ newPosition = 0 @>

    member __.``roundRobin returns second element when last position is 1`` () =
        let list = [1; 2]
        let lastPostion = 1
        test
            <@
                let (_, element) = Seq.roundRobin lastPostion list
                element = Seq.last list
            @>

    member __.``roundRobin returns next position as last position plus one`` () =
        let list = [1; 2]
        let lastPostion = 2
        test
            <@
                let (newPosition, _) = Seq.roundRobin lastPostion list
                newPosition = lastPostion + 1
            @>
