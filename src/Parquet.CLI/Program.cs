using System;
using Cpf.App;
using Parquet.CLI.Commands;

namespace Parquet.CLI
{
   /// <summary>
   /// This is a parquet.net dotnet CLI tool that can be installed globally. An ultimate replacement for "parq".
   /// It is very much in progress and in design.
   /// </summary>
   class Program
   {
      static int Main(string[] args)
      {
         var app = new Application("Parquet CLI (https://github.com/elastacloud/parquet-dotnet)");

         app.Command("schema", cmd =>
         {
            cmd.Description = Help.Command_Schema_Description;

            Argument<string> path = cmd.Argument<string>("path", Help.Command_Schema_Path).Required();

            cmd.OnExecute(() =>
            {
               new SchemaCommand(path.Value).Execute();
            });
         });

         return app.Execute();
      }
   }
}