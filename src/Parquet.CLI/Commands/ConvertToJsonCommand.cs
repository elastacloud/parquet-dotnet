using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class ConvertToJsonCommand : FileInputCommand
   {
      private const ConsoleColor BracketColor = ConsoleColor.Yellow;
      private const ConsoleColor NameColor = ConsoleColor.DarkGray;

      public ConvertToJsonCommand(string path) : base(path)
      {
      }

      public void Execute()
      {
         var walker = new JsonWalker(ReadTable());
         walker.Walk();
      }

      private class JsonWalker : TableWalker
      {
         public JsonWalker(Table table) : base(table)
         {
         }

         protected override void OpenTable()
         {
            Write("{[", BracketColor);
         }

         protected override void CloseTable()
         {
            WriteLine();
            Write("]}", BracketColor);
         }

         protected override void OpenRow(Field f, Row row, int level, bool isFirst, bool isLast)
         {
            //Console.Write("OR" + f?.Name);

            if(isFirst)
            {
               
            }
            else
            {
               Write(",");
            }

            WriteLine();

            Ident(level);

            if(f != null)
            {
               Write("\"", NameColor);
               Write(f.Name, NameColor);
               Write("\": ", NameColor);
            }

            Write("{", BracketColor);
         }

         protected override void CloseRow(Field f, Row row, int level, bool isFirst, bool isLast)
         {
            WriteLine();
            Ident(level);
            Write("}", BracketColor);
            //Console.Write("CR" + f?.Name);
         }

         protected override void OpenValue(Field f, object value, int level, bool isFirst, bool isLast)
         {
            //Write("OV");
            //Ident(level + 1);

            if(isFirst)
            {
               WriteLine();
               Ident(level + 1);
            }
            else
            {
               Write(", ");
            }

            if (f != null)
            {
               Write("\"", NameColor);
               Write(f.Name, NameColor);
               Write("\": ", NameColor);
            }

            if(value is string)
            {
               Write("\"");
            }

            if (value == null)
            {
               Write("null");
            }
            else
            {
               Write(value.ToString());
            }

            if (value is string)
            {
               Write("\"");
            }
         }

         private void Ident(int level)
         {
            string s = new string(' ', (level + 1) * 2);

            Write(s);
         }

      }
   }
}
