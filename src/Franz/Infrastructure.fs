namespace Franz.Internal

/// Type alias for MailBoxProcessor
type Agent<'T> = MailboxProcessor<'T>

[<AutoOpen>]
module AgentExtensions =
    type MailboxProcessor<'T> with
        /// Function to start an agent supervised
        static member StartSupervised (body : MailboxProcessor<_> -> Async<unit>) =
            let watchdog f x = async {
                while true do
                    try
                        do! f x
                    with _ -> ()
            }
            Agent.Start (fun inbox -> watchdog body inbox)

[<AutoOpen>]
module Debug =
    open System.Diagnostics

    /// Function to debug print using formatting
    let dprintfn fmt = Printf.ksprintf Debug.WriteLine fmt

[<AutoOpen>]
module Seq =
    /// Function to handle round robin
    let roundRobin lastPos list =
        let length = list |> Seq.length
        if (lastPos < length - 1) then
            let pos = lastPos + 1
            (pos, list |> Seq.nth pos)
        else
            (0, list |> Seq.head)
