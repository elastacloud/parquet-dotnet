using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cpf;
using Parquet.Data;

namespace Parquet.CLI.Commands
{
   class SchemaCommand
   {
      private readonly PoshConsole _pc;
      private readonly string _path;

      public SchemaCommand(PoshConsole pc, string path)
      {
         _pc = pc;
         _path = path ?? throw new ArgumentNullException(nameof(path));
      }

      public void Execute()
      {
         using (var reader = ParquetReader.OpenFromFile(_path))
         {
            Schema schema = reader.Schema;

            PrintSchema(schema);
         }
      }

      private void PrintSchema(Schema schema)
      {
         foreach(Field field in schema.Fields)
         {
            PrintField(field, 0);
         }
      }

      private void PrintField(Field field, int nesting)
      {
         if(nesting > 0)
         {
            _pc.Write(new string('.', nesting * 2), ConsoleColor.DarkGray);
         }

         _pc.Write(field.Name, ConsoleColor.Green);

         switch (field.SchemaType)
         {
            case SchemaType.Data:
               var df = (DataField)field;
               if (df.IsArray)
               {
                  _pc.Write("[]", ConsoleColor.Yellow);
               }
               if(df.HasNulls)
               {
                  _pc.Write("?", ConsoleColor.White);
               }
               _pc.Write(" ");
               _pc.Write(df.DataType.ToString(), ConsoleColor.Red);
               _pc.Write(" ");
               _pc.Write(df.Path, ConsoleColor.DarkGray);
               
               _pc.WriteLine();
               break;

            case SchemaType.Map:
               var mf = (MapField)field;
               _pc.Write(" (map)", ConsoleColor.Yellow);
               _pc.WriteLine();
               PrintField(mf.Key, nesting + 1);
               PrintField(mf.Value, nesting + 1);
               break;

            case SchemaType.Struct:
               var sf = (StructField)field;
               _pc.Write(" (struct)", ConsoleColor.Yellow);
               _pc.WriteLine();
               foreach(Field f in sf.Fields)
               {
                  PrintField(f, nesting + 1);
               }
               break;

            default:
               _pc.WriteLine(field.SchemaType.ToString() + "?", ConsoleColor.Yellow);
               break;
         }
      }
   }
}
