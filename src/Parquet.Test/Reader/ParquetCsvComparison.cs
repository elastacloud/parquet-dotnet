using System.IO;
using System.Reflection;
using System.Text;
using Path = System.IO.Path;
using F = System.IO.File;
using Type = System.Type;
using NetBox.FileFormats;
using System.Collections.Generic;
using Xunit;

namespace Parquet.Test.Reader
{
   public class ParquetCsvComparison
   {
      protected void CompareFiles(string baseName, params Type[] columnTypes)
      {
         ParquetDataSet parquet = ReadParquet(baseName + ".parquet");
         var csv = ReadCsv(baseName + ".csv");
         Compare(parquet, csv, columnTypes);
      }

      private void Compare(ParquetDataSet parquet, Dictionary<string, List<string>> csv, Type[] columnTypes)
      {
         //compar number of columns is the same
         Assert.Equal(parquet.Columns.Count, csv.Count);

         //compare column names
         foreach(ParquetColumn pq in parquet.Columns)
         {
            Assert.True(csv.ContainsKey(pq.Name));
         }

         //compare column values one by one
         Assert.True(columnTypes.Length == csv.Count, $"incorrect type count, expected {csv.Count} but found {columnTypes.Length}");

         //compare individual columns
         int i = 0;
         foreach(ParquetColumn pc in parquet.Columns)
         {
            List<string> cc = csv[pc.Name];
            Type expectedColumnType = columnTypes[i++];

            //validate column type
            Assert.True(expectedColumnType == pc.SystemType, $"expected {expectedColumnType} for column {pc.Name} but found {pc.SystemType}");
         }
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
