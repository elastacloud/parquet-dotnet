using Parquet.Data;
using Parquet.Formats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Xunit;

namespace Parquet.Test.Formats
{
   public class CsvFormatTest
   {
      [Fact]
      public void Reads_simple_csv()
      {
         using (var csvs = System.IO.File.OpenRead(GetDataFilePath("alltypes.csv")))
         {
            var ds = new DataSet();
            ds.ReadCsv(csvs, new CsvOptions { InferSchema = true, HasHeaders = true });


         }
      }

      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}
