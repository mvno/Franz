module RecoveryTest

open Swensen.Unquote
open Cluster
open Franz
open Franz.HighLevel
open System.Threading

let topicName = "Franz.Integration.Test"

[<FranzFact>]
let ``producer can handle rolling cluster restart``() =
    reset()
    
    let producer = new RoundRobinProducer(kafka_brokers)
    let message = { Key = ""; Value = "Test" }
    producer.SendMessage(topicName, message)

    shutdown_a_kafka 1
    start_a_kafka 1
    shutdown_a_kafka 2
    start_a_kafka 2
    shutdown_a_kafka 3
    start_a_kafka 3

    producer.SendMessage(topicName, message)
