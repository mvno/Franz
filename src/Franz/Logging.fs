namespace Franz

open System
open Franz.Internal

/// Logger interface
type ILogger =
    inherit IDisposable
    abstract member Trace : Action<string> with get, set
    abstract member TraceWithException : Action<string, exn> with get, set
    abstract member Info : Action<string> with get, set
    abstract member InfoWithException : Action<string, exn> with get, set
    abstract member Warning : Action<string> with get, set
    abstract member WarningWithException : Action<string, exn> with get, set
    abstract member Error : Action<string, exn> with get, set
    abstract member Fatal : Action<string, exn> with get, set


/// The default logger. This logger writes to debug, standard and error output.
type DefaultLogger() =
    member val Trace = new Action<string>(fun x -> dprintfn "TRACE: %s" x) with get, set
    member val TraceWithException = new Action<string, exn>(fun x y -> dprintfn "TRACE: %s\r\n%s" x (y.ToString())) with get, set
    member val Info = new Action<string>(fun x -> printfn "INFO: %s" x) with get, set
    member val InfoWithException = new Action<string, exn>(fun x y -> printfn "INFO: %s\r\n%s" x (y.ToString())) with get, set
    member val Warning = new Action<string>(fun x -> printfn "WARNING: %s" x) with get, set
    member val WarningWithException = new Action<string, exn>(fun x y -> printfn "WARNING: %s\r\n%s" x (y.ToString())) with get, set
    member val Error = new Action<string, exn>(fun x y -> eprintfn "ERROR: %s\r\n%s" x (y.ToString())) with get, set
    member val Fatal = new Action<string, exn>(fun x y -> eprintfn "FATAL: %s\r\n%s" x (y.ToString())) with get, set

    interface ILogger with
        member self.Trace with get() = self.Trace and set(x) = self.Trace <- x
        member self.TraceWithException with get() = self.TraceWithException and set(x) = self.TraceWithException <- x
        member self.Info with get() = self.Info and set(x) = self.Info <- x
        member self.InfoWithException with get() = self.InfoWithException and set(x) = self.InfoWithException <- x
        member self.Warning with get() = self.Warning and set(x) = self.Warning <- x
        member self.WarningWithException with get() = self.WarningWithException and set(x) = self.WarningWithException <- x
        member self.Error with get() = self.Error and set(x) = self.Error <- x
        member self.Fatal with get() = self.Fatal and set(x) = self.Fatal <- x

    interface IDisposable with
        member __.Dispose() = ()

/// Log configuration
[<Sealed;AbstractClass>]
type LogConfiguration private() =
    static let mutable logger = (new DefaultLogger()) :> ILogger
    
    /// Get or set the logger. This method is not thread-safe and should be changed before connecting to Kafka.
    static member Logger with get() = logger and set(x) = logger <- x
