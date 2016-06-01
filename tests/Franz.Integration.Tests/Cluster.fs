module Cluster
    open System
    open System.Diagnostics
    open System.IO
    open System.Threading
    open System.Text
    open Xunit
    open Franz
    open Franz.HighLevel

    let kafka_brokers = [|
        { Address = "http://192.168.100.100"; Port = 9091 };
        { Address = "http://192.168.100.100"; Port = 9092 };
        { Address = "http://192.168.100.100"; Port = 9093 };|]

    let zookeepers = [|
        { Address = "http://192.168.100.100"; Port = 2181 };
        { Address = "http://192.168.100.100"; Port = 2182 };
        { Address = "http://192.168.100.100"; Port = 2183 };|]

    let sshPath = "C:\\Program Files\\OpenSSH-Win64\\ssh.exe"
    let virtualBoxPath = "C:\\Program Files\\Oracle\\VirtualBox\\VBoxManage.exe"
    let vagrantPath = "C:\\HashiCorp\\Vagrant\\bin\\vagrant.exe"

    let getNewPath = System.Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Machine)

    let executeCommandInShell (command : string) (arguments : string) =
        let startInfo = new ProcessStartInfo()
        startInfo.FileName <- command
        startInfo.Arguments <- arguments
        startInfo.UseShellExecute <- true
        startInfo.CreateNoWindow <- true
        startInfo.WindowStyle <- ProcessWindowStyle.Hidden

        let proc = Process.Start(startInfo)
        proc.WaitForExit()
        proc.ExitCode

    let executeCommandOutsideShell (command : string) (arguments : string) =
        let startInfo = new ProcessStartInfo()
        startInfo.FileName <- command
        startInfo.Arguments <- arguments
        startInfo.UseShellExecute <- false
        startInfo.CreateNoWindow <- true
        startInfo.WindowStyle <- ProcessWindowStyle.Hidden
        startInfo.EnvironmentVariables.Item("PATH") <- getNewPath
        startInfo.RedirectStandardOutput <- true
        startInfo.RedirectStandardError <- true

        let outputBuilder = StringBuilder()
        let errorBuilder = StringBuilder() 
        let outputHandle = new AutoResetEvent(false)
        let errorHandle = new AutoResetEvent(false)

        let proc = new Process()
        proc.StartInfo <- startInfo
        proc.OutputDataReceived.Add(fun x -> if x.Data = null then outputHandle.Set() |> ignore
                                             else outputBuilder.AppendLine(x.Data) |> ignore)
        proc.ErrorDataReceived.Add(fun x -> if x.Data = null then errorHandle.Set() |> ignore
                                            else errorBuilder.AppendLine(x.Data) |> ignore)
        proc.Start() |> ignore
        proc.BeginOutputReadLine()
        proc.BeginErrorReadLine()

        proc.WaitForExit()
        outputHandle.WaitOne(1000) |> ignore
        errorHandle.WaitOne(1000) |> ignore

        Console.WriteLine(outputBuilder.ToString())
        Console.WriteLine(errorBuilder.ToString())

        proc.ExitCode

    let failIfError command exitCode =
        match exitCode with
        | 0 -> ()
        | _ -> failwith(sprintf "'%s' - failed" command)

    let performVagrantCommand (command : string) =
        executeCommandOutsideShell vagrantPath command |> ignore
        ()

    let performSSHCommand (command : string) =
        if Environment.GetEnvironmentVariable("APPVEYOR") = null then
            executeCommandInShell vagrantPath ("ssh -- " + command) |> failIfError command
        else
            executeCommandOutsideShell vagrantPath ("ssh -- " + command) |> failIfError command

    let reset() =
        performSSHCommand "sudo ./reset_cluster.sh"

    let kill() =
        performSSHCommand "sudo ./kill_cluster.sh"

    let stop() =
        performSSHCommand "sudo ./stop_cluster.sh"

    let start() =
        performSSHCommand "sudo ./start_cluster.sh"

    let kill_random_kafka() =
        performSSHCommand "sudo ./kill_random_kafka.sh"

    let kill_random_zookeeper() =
        performSSHCommand "sudo ./kill_random_zookeeper.sh"

    let shutdown_random_kafka() =
        performSSHCommand "sudo ./shutdown_random_kafka.sh"

    let shutdown_random_zookeeper() =
        performSSHCommand "sudo ./shutdown_random_zookeeper.sh"

    let kill_a_kafka(id : int) =
        performSSHCommand (sprintf "sudo ./kill_a_kafka.sh %i" id)

    let kill_a_zookeeper(id : int) =
        performSSHCommand (sprintf "sudo ./kill_a_zookeeper.sh %i" id)

    let shutdown_a_kafka(id : int) =
        performSSHCommand (sprintf "sudo ./shutdown_a_kafka.sh %i" id)

    let shutdown_a_zookeeper(id : int) =
        performSSHCommand (sprintf "sudo ./shutdown_a_zookeeper.sh %i" id)

    let installPrerequest(name : string) (expectedFile : string) =
        if File.Exists(expectedFile) then
            executeCommandOutsideShell "choco" ("upgrade " + name + " -y") |> ignore
        else
            executeCommandOutsideShell "choco" ("install " + name + " -y") |> ignore

    let ensureClusterIsRunning =
        System.AppDomain.CurrentDomain.DomainUnload.Add(fun x -> performVagrantCommand "destroy -f")

        installPrerequest "win32-openssh" sshPath
        installPrerequest "virtualbox" virtualBoxPath
        installPrerequest "vagrant" vagrantPath

        if (executeCommandOutsideShell virtualBoxPath "showvminfo kafka_cluster") = 1 then
            executeCommandOutsideShell virtualBoxPath "unregistervm kafka_cluster --delete" |> ignore

        performVagrantCommand "destroy -f"
        if File.Exists("Vagrantfile") then File.Delete("Vagrantfile")
        performVagrantCommand "init ChristianTrolleMikkelsen/kafkacluster"
        performVagrantCommand "box update"
        performVagrantCommand "up"

    [<System.AttributeUsageAttribute(AttributeTargets.Method, AllowMultiple = false)>]
    type FranzFactAttribute()=
        inherit Xunit.FactAttribute()
        let cluster = ensureClusterIsRunning
