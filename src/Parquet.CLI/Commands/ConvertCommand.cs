using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Parquet.Data.Rows;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class ConvertCommand : FileInputCommand
   {
      private readonly string _path;

      public ConvertCommand(string path) : base(path)
      {
         _path = path;
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
         Table t = ReadTable();

         string json = t.ToString("j");

         WriteLine(json);

         //object jsonObject = JsonConvert.DeserializeObject(json);
         //WriteColor(jsonObject, 0);
      }

      private const ConsoleColor BracketColor = ConsoleColor.DarkGray;
      private const ConsoleColor QuoteColor = ConsoleColor.Yellow;

      private void WriteColor(object jsonObject, int level)
      {
         string ident = new string(' ', level * 2);

         if(jsonObject is JProperty jp)
         {
            Write(ident);
            Write("\"", QuoteColor);
            Write(jp.Name);
            Write("\": ", QuoteColor);

            WriteColor(jp.Value, level + 1);
         }
         else if(jsonObject is JValue jv)
         {
            WriteLine(jv);
         }
         else if (jsonObject is JArray jar)
         {
            Write(ident);
            Write("[", BracketColor);
            WriteLine();

            foreach(object element in jar)
            {
               WriteColor(element, level + 1);
            }

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

            Write(ident);
            Write("}", BracketColor);
            WriteLine();
         }
         else
         {
            throw new NotSupportedException(jsonObject.GetType().ToString());
         }
      }


   }
}