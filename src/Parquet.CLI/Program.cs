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
         var app = new Application("Parquet CLI");

         app.Command("schema", cmd =>
         {
            cmd.Description = "Displays Parquet file schema";

            Argument<string> path = cmd.Argument<string>("path").IsRequired();

            cmd.OnExecute(pc =>
            {
               new SchemaCommand(pc, path.Value).Execute();
            });
         });

         return app.Execute();
      }
   }
}