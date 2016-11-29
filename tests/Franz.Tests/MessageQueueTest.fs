namespace Franz.Tests

open Franz.HighLevel
open Xunit
open Swensen.Unquote
open System.Threading

type MessageQueueTest() =
    [<Fact>]
    let ``queue is empty by default``() =
        let queue = new MessageQueue()
        let elementReceivedEvent = new ManualResetEvent(false)
        queue.Start()
        async {
            queue |> Seq.tryHead |> ignore
            elementReceivedEvent.Set() |> ignore
        } |> Async.Start
        
        test <@ elementReceivedEvent.WaitOne(1000) |> not @>

    [<Fact>]
    let ``if queue has element then it is returned``() =
        let expectedCrc = 12
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = expectedCrc; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Start()

        test
            <@
                let elm = queue |> Seq.tryHead
                queue.Stop()
                if elm.IsSome && elm.Value.Message.Crc = expectedCrc then true else false
            @>

    [<Fact>]
    let ``exception is thrown if fatal exception has been set on queue``() =
        let expectedException = new System.Exception("Test exception")
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Start()
        queue.SetFatalException(expectedException)

        raisesWith<System.Exception> <@ queue |> Seq.tryHead |> ignore @> (fun e -> <@ e.Message = expectedException.Message @>)

    [<Fact>]
    let ``nothing is returned if queue is paused``() =
        let expectedCrc = 12
        let pausedEvent = new ManualResetEvent(false)
        let mutable returnedElement = None
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = expectedCrc; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Start()
        async {
            queue |> Seq.head |> ignore
            queue.Pause()
            pausedEvent.Set() |> ignore
            returnedElement <- Some (queue |> Seq.head)
        } |> Async.Start

        test <@ pausedEvent.WaitOne(1000) @>
        Thread.Sleep(1000)
        test <@ returnedElement.IsNone @>

    [<Fact>]
    let ``elements is returned after queue has been paused and started again``() =
        let expectedCrc = 12
        let pausedEvent = new ManualResetEvent(false)
        let startedEvent = new ManualResetEvent(false)
        let completeEvent = new ManualResetEvent(false)
        let mutable returnedElement = None
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = expectedCrc; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Start()
        async {
            queue |> Seq.head |> ignore
            queue.Pause()
            pausedEvent.Set() |> ignore
            do! Async.Sleep(500)
            queue.Start()
            startedEvent.Set() |> ignore
            returnedElement <- Some (queue |> Seq.head)
            completeEvent.Set() |> ignore
        } |> Async.Start

        test <@ pausedEvent.WaitOne(1000) @>
        test <@ startedEvent.WaitOne(1000) @>
        test <@ completeEvent.WaitOne(1000) @>
        test <@ returnedElement.IsSome && returnedElement.Value.Message.Crc = expectedCrc @>

    [<Fact>]
    let ``adding elements to queue while consuming returns added data``() =
        let expectedCrc = 12
        let completedEvent = new ManualResetEvent(false)
        let mutable returnedElement = None
        let queue = new MessageQueue()
        queue.Start()
        async {
            returnedElement <- Some (queue |> Seq.head)
            completedEvent.Set() |> ignore
        } |> Async.Start
        Thread.Sleep(1000)
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = expectedCrc; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        
        test <@ completedEvent.WaitOne(1000) @>
        test <@ returnedElement.IsSome && returnedElement.Value.Message.Crc = expectedCrc @>

    [<Fact>]
    let ``consuming all elements in queue raises the queue empty event``() =
        let elementConsumed = new ManualResetEvent(false)
        let eventReceived = new ManualResetEvent(false)
        let queue = new MessageQueue()
        queue.QueueEmpty.Add (fun _ -> eventReceived.Set() |> ignore)
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Start()
        async {
            queue |> Seq.head |> ignore
            elementConsumed.Set() |> ignore
            queue |> Seq.head |> ignore
        } |> Async.Start

        test <@ elementConsumed.WaitOne(1000) @>
        test <@ eventReceived.WaitOne(1000) @>

    [<Fact>]
    let ``it is possible to find the lowest unprocessed partition offsets``() =
        let lowestOffsetPartition0 = 8L
        let lowestOffsetPartition1 = 5L
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 10L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 5L; PartitionId = 1 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 8L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 17L; PartitionId = 1 })
        
        let partitifionOffsets = queue.FindLowestUnprocessedPartitionOffsets()

        test <@ partitifionOffsets |> Seq.length = 2 @>
        test <@ partitifionOffsets |> Seq.exists (fun x -> x.Offset = lowestOffsetPartition0 && x.PartitionId = 0) @>
        test <@ partitifionOffsets |> Seq.exists (fun x -> x.Offset = lowestOffsetPartition1 && x.PartitionId = 1) @>

    [<Fact>]
    let ``clear removes all data from the queue``() =
        let elementReceivedEvent = new ManualResetEvent(false)
        let queue = new MessageQueue()
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        queue.Add({ Message = { Key = [||]; Value = [||]; Crc = 0; MagicByte = 0y; Attributes = 0y }; Offset = 0L; PartitionId = 0 })
        
        queue.Clear()

        queue.Start()
        async {
            queue |> Seq.tryHead |> ignore
            elementReceivedEvent.Set() |> ignore
        } |> Async.Start
        
        test <@ elementReceivedEvent.WaitOne(1000) |> not @>