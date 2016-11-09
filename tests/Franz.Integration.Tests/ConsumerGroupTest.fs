module ConsumerGroupTest

open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel
open System.Threading

let topicName = "Franz.Integration.Test"

let startConsumingAsync token (consumer : IConsumer) =
    Async.Start(async { consumer.Consume(token) |> ignore }, token)

[<FranzFact>]
let ``consumer group consumer must be able to read 1 message`` () =
    reset()
    createTopic topicName 1 1

    let broker = new BrokerRouter(kafka_brokers, 10000)
    use producer = new RoundRobinProducer(broker)
    let expectedMessage = {Key = ""; Value = "must produce and consume 1 message"}
    producer.SendMessage(topicName, expectedMessage);
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    use consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()

    tokenSource.CancelAfter(30000)

    let message = consumer.Consume(tokenSource.Token) |> Seq.tryHead
    test
        <@
            match message with
            | Some x -> System.Text.Encoding.UTF8.GetString(x.Message.Value) = expectedMessage.Value
            | None -> false
        @>

[<FranzFact>]
let ``consumer group consumer must be able to read message after starting`` () =
    reset()
    createTopic topicName 1 1

    let broker = new BrokerRouter(kafka_brokers, 10000)
    use producer = new RoundRobinProducer(broker)
    let expectedMessage = {Key = ""; Value = "must produce and consume 1 message"}
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    use consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let resetEvent = new ManualResetEvent(false)

    async {
        let message = consumer.Consume(tokenSource.Token) |> Seq.head
        resetEvent.Set() |> ignore
        test <@ System.Text.Encoding.UTF8.GetString(message.Message.Value) = expectedMessage.Value @>
    } |> Async.Start

    producer.SendMessage(topicName, expectedMessage);

    test<@ resetEvent.WaitOne(30000) @>

[<FranzFact>]
let ``with one consumer all partitions is assigned to the consumer`` () =
    reset()
    createTopic topicName 3 3

    let broker = new BrokerRouter(kafka_brokers, 10000)
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    use consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)

    consumer |> startConsumingAsync tokenSource.Token

    use s = consumer.OnConnected.Subscribe(fun x ->
        let assignment = x.Assignment.PartitionAssignment |> Seq.head
        let availablePartitionIds = broker.GetAvailablePartitionIds(topicName)
        test <@ assignment.Topic = topicName && assignment.Partitions |> Seq.exists (fun x -> availablePartitionIds |> Seq.contains x) @>
        completedEvent.Set() |> ignore)

    test <@ completedEvent.WaitOne(30000) @>

[<FranzFact>]
let ``with two consumers partitions are split among them`` () =
    reset()
    createTopic topicName 2 2

    let broker = new BrokerRouter(kafka_brokers, 10000)
    broker.Connect()
    let availablePartitionIds = broker.GetAvailablePartitionIds(topicName)
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer1 = new GroupConsumer(broker, options)
    use consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)

    consumer1 |> startConsumingAsync tokenSource.Token
    consumer2 |> startConsumingAsync tokenSource.Token

    use s = consumer2.OnConnected.Subscribe(fun x ->
        let assignment = x.Assignment.PartitionAssignment |> Seq.head
        if assignment.Partitions |> Seq.length = 1 then
            test
                <@
                    let partitionId = assignment.Partitions |> Seq.exactlyOne
                    assignment.Topic = topicName && availablePartitionIds |> Seq.contains partitionId
                @>
            completedEvent.Set() |> ignore)

    test <@ completedEvent.WaitOne(60000) @>

[<FranzFact>]
let ``when a consumer leaves the group another consumer takes over the partitions`` () =
    reset()
    createTopic topicName 2 2

    let broker = new BrokerRouter(kafka_brokers, 10000)
    broker.Connect()
    let availablePartitionIds = broker.GetAvailablePartitionIds(topicName)
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer1 = new GroupConsumer(broker, options)
    use consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let tokenSource2 = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)
    let countDownEvent = new CountdownEvent(2)

    consumer1 |> startConsumingAsync tokenSource.Token
    use t1 = consumer1.OnConnected.Subscribe (fun x ->
        if not countDownEvent.IsSet then
            countDownEvent.Signal() |> ignore
        else
            let assignment = x.Assignment.PartitionAssignment |> Seq.head
            let availablePartitionIds = broker.GetAvailablePartitionIds(topicName)
            test <@ assignment.Topic = topicName && assignment.Partitions |> Seq.forall (fun x -> availablePartitionIds |> Seq.contains x) @>
            completedEvent.Set() |> ignore)
    
    consumer2 |> startConsumingAsync tokenSource.Token
    use t2 = consumer2.OnConnected.Subscribe (fun _ ->
        if not countDownEvent.IsSet then
            countDownEvent.Signal() |> ignore
        else ())

    if countDownEvent.Wait(60000) then
        consumer2.LeaveGroup()
    else failwith "Both consumers did not connect"

    test <@ completedEvent.WaitOne(30000) @>

[<FranzFact>]
let ``when initial join fails offsets are not committed`` () =
    reset()
    createTopic topicName 1 1

    let broker = new BrokerRouter(kafka_brokers, 10000)
    broker.Connect()
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let connectedEvent = new ManualResetEventSlim(false)
    let offsetsCommitted = new ManualResetEvent(false)

    use t1 = consumer.OnConnected.Subscribe(fun _ -> connectedEvent.Set())
    use t2 = consumer.OffsetManager.OnOffsetsCommitted.Subscribe(fun x ->
        test <@ connectedEvent.IsSet @>
        offsetsCommitted.Set() |> ignore)
    consumer |> startConsumingAsync tokenSource.Token

    test <@ not <| offsetsCommitted.WaitOne(30000) @>

[<FranzFact>]
let ``when consumer leaves group offsets are committed`` () =
    reset()
    createTopic topicName 1 1

    let broker = new BrokerRouter(kafka_brokers, 10000)
    broker.Connect()
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let connectedEvent = new ManualResetEventSlim(false)
    let offsetsCommitted = new ManualResetEvent(false)

    use t1 = consumer.OnConnected.Subscribe(fun _ ->
        connectedEvent.Set()
        consumer.LeaveGroup())
    use t2 = consumer.OffsetManager.OnOffsetsCommitted.Subscribe(fun x ->
        test <@ connectedEvent.IsSet @>
        test <@ x.ConsumerGroup = options.GroupId @>
        offsetsCommitted.Set() |> ignore)
    consumer |> startConsumingAsync tokenSource.Token

    test <@ offsetsCommitted.WaitOne(30000) @>

[<FranzFact>]
let ``when a consumer group is rebalanced because another consumer connected, then offsets from the connected consumer are comitted`` () =
    reset()
    createTopic topicName 2 2

    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer1 = new GroupConsumer(kafka_brokers, options)
    use consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let firstConsumerConnectedEvent = new ManualResetEventSlim(false)
    let offsetsCommitted = new ManualResetEvent(false)

    use t1 = consumer1.OnConnected.Subscribe(fun _ ->
        if not firstConsumerConnectedEvent.IsSet then
            firstConsumerConnectedEvent.Set()
            printfn "Starting second consumer"
            consumer2 |> startConsumingAsync tokenSource.Token)
    use t2 = consumer1.OffsetManager.OnOffsetsCommitted.Subscribe(fun x ->
        printfn "Consumer1 offsets committed"
        if firstConsumerConnectedEvent.IsSet then
            test <@ x.PartitionOffsets |> Seq.length = 2 @>
            offsetsCommitted.Set() |> ignore)
    consumer1 |> startConsumingAsync tokenSource.Token

    test <@ offsetsCommitted.WaitOne(30000) @>

[<FranzFact>]
let ``offsets are only committed for assigned partitions`` () =
    reset()
    createTopic topicName 2 2

    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer1 = new GroupConsumer(kafka_brokers, options)
    use consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let firstConsumerConnectedEvent = new ManualResetEventSlim(false)
    let secondConsumerConnectedEvent = new ManualResetEventSlim(false)
    let countDownEvent = new CountdownEvent(2)
    let mutable assignedPartitions : PartitionAssignment array = [||]

    use t1 = consumer1.OnConnected.Subscribe(fun _ ->
        if not firstConsumerConnectedEvent.IsSet then
            firstConsumerConnectedEvent.Set()
            consumer2 |> startConsumingAsync tokenSource.Token)
    use t2 = consumer2.OnConnected.Subscribe(fun x ->
        secondConsumerConnectedEvent.Set()
        if firstConsumerConnectedEvent.IsSet && secondConsumerConnectedEvent.IsSet then
            assignedPartitions <- x.Assignment.PartitionAssignment
            consumer2.LeaveGroup())
    use t3 = consumer2.OffsetManager.OnOffsetsCommitted.Subscribe(fun c ->
        let assignedPartitionIds = assignedPartitions |> Seq.map (fun p -> p.Partitions) |> Seq.concat
        let topic = assignedPartitions |> Seq.map (fun p -> p.Topic) |> Seq.head
        let committedPartitionIds = c.PartitionOffsets |> Seq.map (fun p -> p.PartitionId)
        test <@ committedPartitionIds |> Seq.forall (fun p -> assignedPartitionIds |> Seq.contains p) && c.Topic = topic @>
        consumer1.LeaveGroup()
        countDownEvent.Signal() |> ignore)
    use t4 = consumer1.OffsetManager.OnOffsetsCommitted.Subscribe(fun c ->
        if secondConsumerConnectedEvent.IsSet then
            let assignedPartitionIds = assignedPartitions |> Seq.map (fun p -> p.Partitions) |> Seq.concat
            let topic = assignedPartitions |> Seq.map (fun p -> p.Topic) |> Seq.head
            let committedPartitionIds = c.PartitionOffsets |> Seq.map (fun p -> p.PartitionId)
            test <@ committedPartitionIds |> Seq.forall (fun p -> assignedPartitionIds |> Seq.contains p |> not) && c.Topic = topic @>
            countDownEvent.Signal() |> ignore)
    consumer1 |> startConsumingAsync tokenSource.Token
    
    test <@ countDownEvent.Wait(30000) @>

[<FranzFact>]
let ``when a consumer leaves the group ungraceful, the group rebalances`` () =
    reset()
    createTopic topicName 2 2

    let broker = new BrokerRouter(kafka_brokers, 10000)
    broker.Connect()
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    options.SessionTimeout <- 10000
    use consumer1 = new GroupConsumer(broker, options)
    use consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)
    let countDownEvent = new CountdownEvent(2)

    consumer1 |> startConsumingAsync tokenSource.Token
    use t1 = consumer1.OnConnected.Subscribe (fun x ->
        if not countDownEvent.IsSet then
            countDownEvent.Signal() |> ignore
        else
            let assignment = x.Assignment.PartitionAssignment |> Seq.head
            let availablePartitionIds = broker.GetAvailablePartitionIds(topicName)
            test <@ assignment.Topic = topicName && assignment.Partitions |> Seq.forall (fun x -> availablePartitionIds |> Seq.contains x) @>
            completedEvent.Set() |> ignore)
    
    consumer2 |> startConsumingAsync tokenSource.Token
    use t2 = consumer2.OnConnected.Subscribe (fun _ ->
        if not countDownEvent.IsSet then
            countDownEvent.Signal() |> ignore
        else ())

    if countDownEvent.Wait(60000) then
        consumer2.Dispose()
    else failwith "Both consumers did not connect"

    test <@ completedEvent.WaitOne(30000) @>
