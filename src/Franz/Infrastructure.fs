namespace Franz.Internal

/// Type alias for MailBoxProcessor
type Agent<'T> = MailboxProcessor<'T>

[<AutoOpen>]
module Seq =
    /// Function to handle round robin
    let roundRobin lastPos list =
        let length = list |> Seq.length
        if (lastPos < length - 1) then
            let pos = lastPos + 1
            (pos, list |> Seq.item pos)
        else
            (0, list |> Seq.head)

module Retry =
    let retryOnException (state : 'a) (onException : exn -> 'a) (f : 'a -> 'b) : 'b =
        try
            state |> f
        with
        | e ->
            let newState = onException(e)
            f(newState)

[<AutoOpen>]
module ExceptionUtilities =
    open Franz

    let raiseWithErrorLog (someExceptionToRaise : exn) =
        LogConfiguration.Logger.Error.Invoke(someExceptionToRaise.Message, someExceptionToRaise)
        raise(someExceptionToRaise)

    let raiseWithFatalLog (someExceptionToRaise : exn) =
        LogConfiguration.Logger.Fatal.Invoke(someExceptionToRaise.Message, someExceptionToRaise)
        raise(someExceptionToRaise)
