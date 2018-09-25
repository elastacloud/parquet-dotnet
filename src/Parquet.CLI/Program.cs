using System;
using Cpf.App;
using LogMagic;
using Parquet.CLI.Commands;

namespace Parquet.CLI
{
   /// <summary>
   /// This is a parquet.net dotnet CLI tool that can be installed globally. An ultimate replacement for "parq".
   /// It is very much in progress and in design.
   /// </summary>
   class Program
   {
      private static readonly ILog log = L.G(typeof(Program));

      static int Main(string[] args)
      {
         var app = new Application("Parquet CLI (https://github.com/elastacloud/parquet-dotnet)");

         L.Config.WriteTo.AzureApplicationInsights("");

         log.Event("launch",
            "Arguments", string.Join(",", args));

         app.Command("schema", cmd =>
         {
            cmd.Description = Help.Command_Schema_Description;

            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();

            cmd.OnExecute(() =>
            {
               new SchemaCommand(path.Value).Execute();
            });
         });

         app.Command("head", cmd =>
         {
            cmd.Description = Help.Command_Head_Description;

            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();
            Option<string> format = cmd.Option<string>("-f|--format", Help.Command_Head_Format);
            Option<int> max = cmd.Option<int>("-m|--max", Help.Command_Head_Max, 100);

            cmd.OnExecute(() =>
            {
               new HeadCommand(path, max).Execute(format);
            });
         });

         app.Command("to-json", cmd =>
         {
            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();

            cmd.OnExecute(() =>
            {
               new ConvertToJsonCommand(path).Execute();
            });
         });

         return app.Execute();
      }
   }
}