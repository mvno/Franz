module BasicTest

open Xunit
open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel
open System.Threading

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``must produce and consume 1 message`` () =
    let broker = new BrokerRouter(kafka_brokers, 10000)
    let producer = new RoundRobinProducer(broker)
    let expectedMessage = {Key = ""; Value = "must produce and consume 1 message"}
    producer.SendMessage(topicName, expectedMessage);
    let consumer = new ChunkedConsumer(kafka_brokers, topicName)
    let token = new CancellationToken()

    let message = consumer.Consume(token) |> Seq.head

    Assert.Equal(expectedMessage.Value, System.Text.Encoding.UTF8.GetString(message.Message.Value))
