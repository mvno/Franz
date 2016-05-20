namespace Franz

open System
open Franz.Internal

/// Logger interface
type ILogger =
    inherit IDisposable
    abstract member Trace : Action<string> with get, set
    abstract member Info : Action<string> with get, set
    abstract member Warning : Action<string, exn> with get, set
    abstract member Error : Action<string, exn> with get, set
    abstract member Fatal : Action<string, exn> with get, set


/// The default logger. This logger writes to debug, standard and error output.
type DefaultLogger() =
    member val Trace = new Action<string>(fun x -> dprintfn "TRACE: %s" x) with get, set
    member val Info = new Action<string>(fun x -> printfn "INFO: %s" x) with get, set
    member val Warning = new Action<string, exn>(fun x y -> if y <> null then eprintfn "WARNING: %s\r\n%s" x (y.ToString()) else eprintfn "WARNING: %s" x) with get, set
    member val Error = new Action<string, exn>(fun x y -> if y <> null then eprintfn "ERROR: %s\r\n%s" x (y.ToString()) else eprintfn "ERROR: %s" x) with get, set
    member val Fatal = new Action<string, exn>(fun x y -> if y <> null then eprintfn "FATAL: %s\r\n%s" x (y.ToString()) else eprintfn "FATAL: %s" x) with get, set

    interface ILogger with
        member self.Trace with get() = self.Trace and set(x) = self.Trace <- x
        member self.Info with get() = self.Info and set(x) = self.Info <- x
        member self.Warning with get() = self.Warning and set(x) = self.Warning <- x
        member self.Error with get() = self.Error and set(x) = self.Error <- x
        member self.Fatal with get() = self.Fatal and set(x) = self.Fatal <- x
        member __.Dispose() = ()

/// Log configuration
[<Sealed;AbstractClass>]
type LogConfiguration private() =
    static let mutable logger = (new DefaultLogger()) :> ILogger
    
    /// Get or set the logger. This method is not thread-safe and should be changed before connecting to Kafka.
    static member Logger with get() = logger and set(x) = logger <- x
