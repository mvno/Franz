namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Franz")>]
[<assembly: AssemblyProductAttribute("Franz")>]
[<assembly: AssemblyDescriptionAttribute("Kafka client for .NET")>]
[<assembly: AssemblyVersionAttribute("2.0.0")>]
[<assembly: AssemblyFileVersionAttribute("2.0.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "2.0.0"
