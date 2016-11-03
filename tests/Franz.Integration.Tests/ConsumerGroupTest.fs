module ConsumerGroupTest

open Xunit
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
    Assert.True(message.IsSome)
