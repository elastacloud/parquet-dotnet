using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Parquet.Data.Rows;
using NetBox.Extensions;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class ConvertCommand : FileInputCommand
   {
      private readonly string _path;
      private readonly bool _noColour;

      public ConvertCommand(string path, bool noColour) : base(path)
      {
         _path = path;
         _noColour = noColour;
      }

      public void Execute()
      {
         string sourceExtension = Path.GetExtension(_path);

         if(sourceExtension != ".parquet")
         {
            throw new ArgumentException($"Don't know how to read {sourceExtension}");
         }

         ConvertFromParquet();
      }

      private void ConvertFromParquet()
      {
         Telemetry.CommandExecuted("convert",
            "path", _path);

         Table t = ReadTable();

         string json = t.ToString("j");

         int i = 0;
         foreach(string jsonLine in json.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
         {
            if (_noColour)
            {
               WriteLine(jsonLine);
            }
            else
            {
               object jsonDoc = JsonConvert.DeserializeObject(jsonLine);

               WriteColor(jsonDoc, 0, i++);
            }
         }
      }

      private const ConsoleColor BracketColor = ConsoleColor.DarkGray;
      private const ConsoleColor QuoteColor = ConsoleColor.Yellow;
      private const ConsoleColor PropertyNameColor = ConsoleColor.DarkGray;
      private const ConsoleColor ValueColor = ConsoleColor.White;

      private void WriteColor(object jsonObject, int level, int index = -1)
      {
         if(index != -1)
         {
            WriteLine();
            PoshWriteLine($"{{document}} {{#}}{{{index}}}", ConsoleColor.Green, ConsoleColor.DarkGray, ConsoleColor.Yellow);
         }

         string ident = new string(' ', level * 2);

         if(jsonObject is JProperty jp)
         {
            Write(ident);
            Write("\"", PropertyNameColor);
            Write(jp.Name, PropertyNameColor);
            Write("\": ", PropertyNameColor);

            WriteColor(jp.Value, level + 1);
         }
         else if(jsonObject is JValue jv)
         {
            if (jv.Parent.Count > 1)
            {
               if (jv.Previous != null)
               {
                  Write(",");
                  WriteLine();
               }
               Write(ident);
            }

            WriteValue(jv);
         }
         else if (jsonObject is JArray jar)
         {
            Write("[", BracketColor);
            WriteLine();

            foreach(object element in jar)
            {
               WriteColor(element, level + 1);
            }

            WriteLine();
            Write(ident);
            Write("]", BracketColor);
            WriteLine();
         }
         else if(jsonObject is JObject jo)
         {
            Write(ident);
            Write("{", BracketColor);
            WriteLine();

            foreach(object element in jo.Children())
            {
               WriteColor(element, level + 1);
            }

            WriteLine();
            Write(ident);
            Write("}", BracketColor);
            WriteLine();
         }
         else
         {
            throw new NotSupportedException(jsonObject.GetType().ToString());
         }
      }

      private void WriteValue(JValue value)
      {
         switch (value.Type)
         {
            //quoted
            case JTokenType.String:
            case JTokenType.Date:
            case JTokenType.Guid:
            case JTokenType.Uri:
            case JTokenType.TimeSpan:
               Write("\"", QuoteColor);
               Write(value.Value, ValueColor);
               Write("\"", QuoteColor);
               break;
            default:
               Write(value.Value, ValueColor);
               break;
         }
      }


   }
}