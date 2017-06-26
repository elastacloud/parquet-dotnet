using System.IO;
using System.Reflection;
using System.Text;
using Path = System.IO.Path;
using F = System.IO.File;
using NetBox.FileFormats;
using System.Collections.Generic;
using Xunit;

namespace Parquet.Test.Reader
{
   public class ParquetCsvComparison
   {
      protected void CompareFiles(string baseName)
      {
         ParquetDataSet parquet = ReadParquet(baseName + ".parquet");
         var csv = ReadCsv(baseName + ".csv");
         Compare(parquet, csv);
      }

      private void Compare(ParquetDataSet parquet, Dictionary<string, List<string>> csv)
      {
         Assert.Equal(parquet.Columns.Count, csv.Count);
      }

      private ParquetDataSet ReadParquet(string name)
      {
         ParquetDataSet parquet;
         using (Stream fs = F.OpenRead(GetDataFilePath(name)))
         {
            using (ParquetReader reader = new ParquetReader(fs))
            {
               parquet = reader.Read();
            }
         }
         return parquet;
      }

      private Dictionary<string, List<string>> ReadCsv(string name)
      {
         var result = new Dictionary<string, List<string>>();
         using (Stream fs = F.OpenRead(GetDataFilePath(name)))
         {
            var reader = new CsvReader(fs, Encoding.UTF8);

            //header
            string[] columnNames = reader.ReadNextRow();
            foreach (string columnName in columnNames) result[columnName] = new List<string>();

            //values
            string[] values;
            while((values = reader.ReadNextRow()) != null)
            {
               for(int i = 0; i < values.Length; i++)
               {
                  result[columnNames[i]].Add(values[i]);
               }
            }
         }
         return result;
      }

      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}
