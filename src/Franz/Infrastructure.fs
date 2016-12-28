namespace Franz

    /// A endpoint
    type EndPoint = 
        { Address : string
          Port : int32 }    

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
            else (0, list |> Seq.head)

    [<AutoOpen>]
    module Array = 
        let rand = new System.Random()
    
        let swap (a : _ []) x y = 
            let tmp = a.[x]
            a.[x] <- a.[y]
            a.[y] <- tmp
    
        // shuffle an array (in-place)
        let shuffle a = Array.iteri (fun i _ -> swap a i (rand.Next(i, Array.length a))) a

    module Retry = 
        let retryOnException (state : 'a) (onException : exn -> 'a) (f : 'a -> 'b) : 'b = 
            try 
                state |> f
            with e -> 
                let newState = onException (e)
                f (newState)

    [<AutoOpen>]
    module ExceptionUtilities = 
        open System
        open Franz
    
        let raiseWithErrorLog (someExceptionToRaise : exn) = 
            LogConfiguration.Logger.Error.Invoke(someExceptionToRaise.Message, someExceptionToRaise)
            raise (someExceptionToRaise)
    
        let raiseWithFatalLog (someExceptionToRaise : exn) = 
            LogConfiguration.Logger.Fatal.Invoke(someExceptionToRaise.Message, someExceptionToRaise)
            raise (someExceptionToRaise)
    
        let raiseIfDisposed (disposed : bool) = 
            if disposed then 
                raiseWithFatalLog 
                    (ObjectDisposedException 
                         "Illegal attempt made by calling af function on type which have been marked as disposed")

    module internal ErrorHandling = 
        type Result<'a, 'b> = 
            | Success of 'a
            | Failure of 'b
    
        let catch f x = 
            try 
                f x |> Success
            with e -> e |> Failure
    
        let fail x = x |> Failure
        let succeed x = x |> Success
    
        let either fSuccess fFailure result = 
            match result with
            | Success x -> fSuccess x
            | Failure x -> fFailure x

    [<AutoOpenAttribute>]
    module Map = 
        let getValues (x : Map<_, _>) = x |> Seq.map (fun x -> x.Value)
        let getKeys (x : Map<_, _>) = x |> Seq.map (fun x -> x.Key)
