using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data.Rows;
using static Cpf.PoshConsole;

namespace Parquet.CLI.Commands
{
   class HeadCommand
   {
      private readonly string _path;
      private readonly int _max;

      public HeadCommand(string path, int max)
      {
         _path = path;
         _max = max;

         if (_max > 100)
            _max = 100;

         if (_max < 1)
            _max = 1;
      }

      public void Execute(string format)
      {
         Write("displaying first ");
         Write(_max.ToString(), T.ActiveTextColor);
         Write(" records...");
         WriteLine();

         using (var reader = ParquetReader.OpenFromFile(_path, new ParquetOptions { TreatByteArrayAsString = true }))
         {
            Table table = reader.ReadAsTable();

            WriteLine(table.ToString("mjn"));
         }

         WriteLine("work in progress!!!!", T.ErrorTextColor);
      }
   }
}
