module ApiVersionTest

open Swensen.Unquote
open Cluster
open Franz.HighLevel
open Franz

[<FranzFact>]
let ``ApiVersion returns available API versions`` () =
    let brokerRouter = new BrokerRouter(kafka_brokers, 1000)
    brokerRouter.Connect()
    let req = new ApiVersionsRequest()

    let res = brokerRouter.TrySendToBroker(req)

    test <@ res.ErrorCode.IsSuccess() @>
    test <@ res.ApiKeyVersions |> Seq.isEmpty |> not @>
