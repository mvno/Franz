namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Franz")>]
[<assembly: AssemblyProductAttribute("Franz")>]
[<assembly: AssemblyDescriptionAttribute("Kafka client for .NET")>]
[<assembly: AssemblyVersionAttribute("3.1.0")>]
[<assembly: AssemblyFileVersionAttribute("3.0.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "3.1.0"
    let [<Literal>] InformationalVersion = "3.1.0"
