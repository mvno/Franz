namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Franz")>]
[<assembly: AssemblyProductAttribute("Franz")>]
[<assembly: AssemblyDescriptionAttribute("Kafka client for .NET")>]
[<assembly: AssemblyVersionAttribute("0.0.4")>]
[<assembly: AssemblyFileVersionAttribute("0.0.4")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.4"
