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
