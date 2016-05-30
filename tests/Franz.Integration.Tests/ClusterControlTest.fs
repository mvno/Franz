module ClusterControlTest

open Xunit
open Swensen.Unquote
open Cluster

[<Fact>]
let ``kill random works`` () =
    ensureClusterIsRunning
    kill_random_kafka()
    kill_random_kafka()
    kill_random_kafka()
    kill_random_zookeeper()
    kill_random_zookeeper()
    kill_random_zookeeper()
    reset()

[<Fact>]
let ``shutdown random works`` () =
    ensureClusterIsRunning
    shutdown_random_kafka()
    shutdown_random_kafka()
    shutdown_random_kafka()
    shutdown_random_zookeeper()
    shutdown_random_zookeeper()
    shutdown_random_zookeeper()
    reset()

[<Fact>]
let ``kill a works`` () =
    ensureClusterIsRunning
    kill_a_kafka(1)
    kill_a_kafka(2)
    kill_a_kafka(3)
    kill_a_zookeeper(1)
    kill_a_zookeeper(2)
    kill_a_zookeeper(3)
    reset()

[<Fact>]
let ``shutdown a works`` () =
    ensureClusterIsRunning
    shutdown_a_kafka(1)
    shutdown_a_kafka(2)
    shutdown_a_kafka(3)
    shutdown_a_zookeeper(1)
    shutdown_a_zookeeper(2)
    shutdown_a_zookeeper(3)
    reset()

[<Fact>]
let ``start/stop works`` () =
    ensureClusterIsRunning
    stop()
    start()
    reset()

[<Fact>]
let ``kill works`` () =
    ensureClusterIsRunning
    kill()
    start()
    reset()

[<Fact>]
let reset () =
    ensureClusterIsRunning
    reset()
