module ClusterControlTest

open Xunit
open Swensen.Unquote
open Cluster

[<FranzFact>]
let ``kill random works`` () =
    kill_random_kafka()
    kill_random_kafka()
    kill_random_kafka()
    kill_random_zookeeper()
    kill_random_zookeeper()
    kill_random_zookeeper()
    reset()

[<FranzFact>]
let ``shutdown random works`` () =
    shutdown_random_kafka()
    shutdown_random_kafka()
    shutdown_random_kafka()
    shutdown_random_zookeeper()
    shutdown_random_zookeeper()
    shutdown_random_zookeeper()
    reset()

[<FranzFact>]
let ``kill a works`` () =
    kill_a_kafka(1)
    kill_a_kafka(2)
    kill_a_kafka(3)
    kill_a_zookeeper(1)
    kill_a_zookeeper(2)
    kill_a_zookeeper(3)
    reset()

[<FranzFact>]
let ``shutdown a works`` () =
    shutdown_a_kafka(1)
    shutdown_a_kafka(2)
    shutdown_a_kafka(3)
    shutdown_a_zookeeper(1)
    shutdown_a_zookeeper(2)
    shutdown_a_zookeeper(3)
    reset()

[<FranzFact>]
let ``start/stop works`` () =
    stop()
    start()
    reset()

[<FranzFact>]
let ``kill works`` () =
    kill()
    start()
    reset()

[<FranzFact>]
let reset () =
    reset()
