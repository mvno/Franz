module RecoveryTest

open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel
open System.Threading
open Franz.Zookeeper
open Franz.Internal

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``producer can handle rolling cluster restart``() =
    reset()
    
    use producer = new RoundRobinProducer(kafka_brokers)
    let message = { Key = ""; Value = "Test" }
    producer.SendMessage(topicName, message)

    shutdown_a_kafka 1
    start_a_kafka 1
    shutdown_a_kafka 2
    start_a_kafka 2
    shutdown_a_kafka 3
    start_a_kafka 3

    producer.SendMessage(topicName, message)

[<FranzFact>]
let ``consumer can handle rolling cluster restart``() =
    reset()

    use producer = new RoundRobinProducer(kafka_brokers)
    let message = { Key = ""; Value = "Test" }
    producer.SendMessage(topicName, message)

    let options = new ConsumerOptions()
    options.Topic <- topicName
    use consumer = new ChunkedConsumer(kafka_brokers, options)
    let cts = new CancellationTokenSource()
    let msgs = consumer.Consume(cts.Token)
    test <@ msgs |> Seq.length = 1 @>

    shutdown_a_kafka 1
    start_a_kafka 1
    shutdown_a_kafka 2
    start_a_kafka 2
    shutdown_a_kafka 3
    start_a_kafka 3
    
    producer.SendMessage(topicName, message)

    let msgs = consumer.Consume(cts.Token)
    test <@ msgs |> Seq.length = 1 @>
