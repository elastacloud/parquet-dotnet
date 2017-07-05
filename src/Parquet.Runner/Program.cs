using System;
using Parquet;
using Parquet.Data;
using LogMagic;

namespace Parquet.Runner
{
   class Program
   {
      static void Main(string[] args)
      {
         L.Config
            .WriteTo.PoshConsole();

         DataSet ds;
         using (var time = new TimeMeasure())
         {
            ds = ParquetReader.ReadFile("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\postcodes.plain.parquet");

            Console.WriteLine("read in {0}", time.Elapsed);
         }

         Console.WriteLine("has {0} rows", ds.RowCount);
      }
   }
}