namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("FSharp.Kafka")>]
[<assembly: AssemblyProductAttribute("FSharp.Kafka")>]
[<assembly: AssemblyDescriptionAttribute("Kafka cient for .NET")>]
[<assembly: AssemblyVersionAttribute("1.0")>]
[<assembly: AssemblyFileVersionAttribute("1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0"
