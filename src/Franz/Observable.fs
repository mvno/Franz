namespace Franz.Observable

open Franz
open Franz.HighLevel
open System
open System.Threading
open Franz.Internal

type private ObservableMessage =
    | Add of IObserver<MessageWithMetadata>
    | Remove of IObserver<MessageWithMetadata>
    | Complete of AsyncReplyChannel<unit>
    | Push of MessageWithMetadata seq * AsyncReplyChannel<unit>
/// Push based consumer, enabling consuming through [Reactive Extensions](http://reactivex.io/) and related.
///
/// As Kafka by definition isn't push based, a chunked consumer is used to simulate push based messages without consuming a lot of memory.
and ObservableConsumer(consumer : ChunkedConsumer) =
    let mutable disposed = false

    let agent = Agent.Start(fun inbox ->
        let rec loop observers = async {
            let! msg = inbox.Receive()
            match msg with
            | Add observer ->
                if observers |> List.contains observer then
                    return! observers |> loop
                else
                    return! observer :: observers |> loop
            | Remove observer -> return! observers |> List.filter (fun x -> x = observer) |> loop
            | Complete reply ->
                for o in observers do o.OnCompleted()
                reply.Reply()
                ()
            | Push (messages, reply) ->
                for m in messages do
                    for o in observers do
                        o.OnNext(m)
                reply.Reply()
                return! observers |> loop
        }
        loop [])

    let pushToAllObsevers messages reply = Push(messages, reply)

    new (brokerSeeds, topicName, consumerOptions : ConsumerOptions) = new ObservableConsumer(new ChunkedConsumer(topicName, consumerOptions, new BrokerRouter(brokerSeeds, consumerOptions.TcpTimeout)))
    new (brokerSeeds, topicName) = new ObservableConsumer(new ChunkedConsumer(brokerSeeds, topicName, new ConsumerOptions()))
    new (brokerRouter, topicName) = new ObservableConsumer(new ChunkedConsumer(topicName, new ConsumerOptions(), brokerRouter))

    member __.Subscribe(observer : IObserver<MessageWithMetadata>) =
        consumer.CheckDisposedState()
        if observer |> isNull then nullArg "observer"
        agent.Post(Add(observer))
        { new IDisposable with member __.Dispose() = agent.Post(Remove(observer)) }

    member __.Consume(cancellationToken : CancellationToken) =
        consumer.CheckDisposedState()
        let consume = async {
            let rec loop() =
                if cancellationToken.IsCancellationRequested then ()
                else
                    consumer.Consume(cancellationToken)
                    |> pushToAllObsevers
                    |> agent.PostAndReply
                    loop()
            loop() }
        Async.Start(consume, cancellationToken)

    member __.StopConsuming() =
        agent.PostAndReply(Complete)
        consumer.Dispose()

    member self.Dispose() =
        if not disposed then
            self.StopConsuming()
            disposed <- true
    
    member __.GetPosition() =
        consumer.GetPosition()

    member __.SetPosition(offsets) =
        consumer.SetPosition(offsets)

    member __.OffsetManager = consumer.OffsetManager

    interface IObservable<MessageWithMetadata> with
        member self.Subscribe(observer) = self.Subscribe(observer)

    interface IDisposable with
        member self.Dispose() = self.Dispose()
