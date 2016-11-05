module ConsumerGroupTest

open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel
open System.Threading

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``consumer group consumer must be able to read 1 message`` () =
    reset()

    let broker = new BrokerRouter(kafka_brokers, 10000)
    let producer = new RoundRobinProducer(broker)
    let expectedMessage = {Key = ""; Value = "must produce and consume 1 message"}
    producer.SendMessage(topicName, expectedMessage);
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    let consumer = new GroupConsumer(broker, options)
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

    let broker = new BrokerRouter(kafka_brokers, 10000)
    let producer = new RoundRobinProducer(broker)
    let expectedMessage = {Key = ""; Value = "must produce and consume 1 message"}
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    let consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let resetEvent = new ManualResetEvent(false)

    async {
        let message = consumer.Consume(tokenSource.Token) |> Seq.head
        resetEvent.Set() |> ignore
        test <@ System.Text.Encoding.UTF8.GetString(message.Message.Value) = expectedMessage.Value @>
    } |> Async.Start

    producer.SendMessage(topicName, expectedMessage);

    test<@ resetEvent.WaitOne(30000) @>

    tokenSource.Cancel()

[<FranzFact>]
let ``with one consumer all partitions is assigned to the consumer`` () =
    reset()
    createTopic topicName 3 3

    let broker = new BrokerRouter(kafka_brokers, 10000)
    let options = new GroupConsumerOptions()
    options.Topic <- topicName
    let consumer = new GroupConsumer(broker, options)
    let tokenSource = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)

    async {
        consumer.Consume(tokenSource.Token) |> ignore
    } |> Async.Start

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
    let consumer1 = new GroupConsumer(broker, options)
    let consumer2 = new GroupConsumer(kafka_brokers, options)
    let tokenSource = new CancellationTokenSource()
    let completedEvent = new ManualResetEvent(false)

    async {
        consumer1.Consume(tokenSource.Token) |> ignore
    } |> Async.Start

    async {
        consumer2.Consume(tokenSource.Token) |> ignore
    } |> Async.Start

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
