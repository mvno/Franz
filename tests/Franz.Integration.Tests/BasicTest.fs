module BasicTest

open Xunit
open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``must produce and consume 1 message`` () =
    let broker = new BrokerRouter(kafka_brokers, 10000)
    let producer = new RoundRobinProducer(broker)
    let expectedMessage = "must produce and consume 1 message"
    producer.SendMessages(topicName, null, [|expectedMessage|]);
    let consumer = new ChunkedConsumer(kafka_brokers, topicName)

    let message = consumer.Consume() |> Seq.head

    Assert.Equal(expectedMessage, System.Text.Encoding.UTF8.GetString(message.Message.Value))
