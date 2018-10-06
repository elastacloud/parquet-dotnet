﻿using System;
using Cpf;
using Cpf.App;
using LogMagic;
using LogMagic.Enrichers;
using Parquet.CLI.Commands;
using Parquet.CLI.Models;
using static Cpf.PoshConsole;

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
         ConfigureTelemetry(app);

         app.OnBeforeExecuteCommand(cmd =>
         {
            PoshWrite("{p}{a}{r}{q} v", ConsoleColor.Yellow, ConsoleColor.Red, ConsoleColor.Green, ConsoleColor.Blue);

            string[] pts = app.Version.Split('.');
            for (int i = 0; i < pts.Length; i++)
            {
               if (i > 0)
                  Write(".", ConsoleColor.DarkGray);

               Write(pts[i], ConsoleColor.Green);
            }
            WriteLine();
            WriteLine();
         });

         app.OnError((cmd, err) =>
         {
            log.Trace("error in command {command}", cmd.Name, err);
         });

         app.Command("schema", cmd =>
         {
            cmd.Description = Help.Command_Schema_Description;

            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();

            cmd.OnExecute(() =>
            {
               new SchemaCommand(path.Value).Execute();
            });
         });

         app.Command("tojson", cmd =>
         {
            cmd.Description = "todo";

            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();

            cmd.OnExecute(() =>
            {
               new ConvertToJsonCommand(path).Execute();
            });
         });

         app.Command("view-all", cmd =>
         {
            cmd.Description = Help.Command_ViewAll_Description;
            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();
            Option<bool> expandCells = cmd.Option<bool>("-e|--expand", Help.Command_ViewAll_Expand, false);
            Option<int> displayMinWidth = cmd.Option<int>("-m|--min", Help.Command_ViewAll_Min, 5);
            Option<bool> displayNulls = cmd.Option<bool>("-n|--nulls", Help.Command_ViewAll_Nulls, false);

            cmd.OnExecute(() =>
            {
               ViewSettings settings = new ViewSettings
               {
                  displayMinWidth = displayMinWidth,
                  displayNulls = displayNulls,
                  displayTypes = false,
                  expandCells = expandCells,
                  truncationIdentifier = string.Empty
               };

               new DisplayFullCommand<Views.FullConsoleView>(path).Execute(settings);
            });
         });

         app.Command("view", cmd =>
         {
            cmd.Description = Help.Command_View_Description;
            Argument<string> path = cmd.Argument<string>("path", Help.Argument_Path).Required();
            Option<bool> expandCells = cmd.Option<bool>("-e|--expand", Help.Command_ViewAll_Expand, false);
            Option<int> displayMinWidth = cmd.Option<int>("-m|--min", Help.Command_ViewAll_Min, 5);
            Option<bool> displayNulls = cmd.Option<bool>("-n|--nulls", Help.Command_ViewAll_Nulls, true);
            Option<bool> displayTypes = cmd.Option<bool>("-t|--types", Help.Command_ViewAll_Types, false);
            Option<string> truncationIdentifier = cmd.Option<string>("-u|--truncate", Help.Command_ViewAll_Types, "...");

            cmd.OnExecute(() =>
            {

               ViewSettings settings = new ViewSettings
               {
                  displayMinWidth = displayMinWidth,
                  displayNulls = displayNulls,
                  displayTypes = displayTypes,
                  expandCells = expandCells,
                  truncationIdentifier = truncationIdentifier,
                  displayReferences = false
               };

               new DisplayFullCommand<Views.InteractiveConsoleView>(path).Execute(settings);
            });
         });

         int exitCode;
         using (L.Context(KnownProperty.OperationId, Guid.NewGuid().ToString()))
         {
            exitCode = app.Execute();
         }

#if DEBUG
         WriteLine("debug: press any key to close");
         Console.ReadKey();
#endif

         return exitCode;
      }

      private static void ConfigureTelemetry(Application app)
      {
         //let's see if we get complains about performance here, should be OK
         L.Config
            .WriteTo.AzureApplicationInsights("0a310ae1-0f93-43fc-bfa1-62e92fc869b9", flushOnWrite: true)
            .EnrichWith.Constant(KnownProperty.Version, app.Version);

      }
   }
}