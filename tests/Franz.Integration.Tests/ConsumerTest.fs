module ConsumerTest

open Swensen.Unquote
open Cluster
open Franz
open System.Threading
open Franz.HighLevel
open Franz.Internal

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``dispose should not throw exception`` () =
    let producer = new RoundRobinProducer(kafka_brokers)
    let expectedMessage = {Key = ""; Value = "Test"}
    producer.SendMessage(topicName, expectedMessage);

    let options = new ConsumerOptions()
    options.Topic <- topicName
    use consumer = new ChunkedConsumer(kafka_brokers, options)
    let cts = new CancellationTokenSource()

    consumer.Consume(cts.Token)
    |> Seq.iter (fun _ -> ())

    test <@ consumer.Dispose(); true @>
