using System;
using System.Linq;
using LogMagic;

namespace parq
{
    class Program
    {
      static ILog _log = L.G<Program>();
      static void Main(string[] args)
        {
         L.Config.WriteTo.PoshConsole();

         if (string.IsNullOrEmpty(AppSettings.Instance.InputFilePath))
         {
            WriteHelp();
         }
         else
         {
            var path = System.IO.Path.Combine(AppContext.BaseDirectory, AppSettings.Instance.InputFilePath);
            _log.D("Input file chosen as {0}", path);

            if (!System.IO.File.Exists(path))
            {
               _log.E("The path {0} does not exist", path);
               return;
            }
            else
            {
               var fileInfo = new System.IO.FileInfo(path);
               _log.I("The file has a length of {0}", fileInfo.Length);

               using (var reader = new Parquet.ParquetReader(fileInfo.Open(System.IO.FileMode.Open)))
               {
                  var dataSet = reader.Read();
                  foreach (var column in dataSet.Columns)
                  {
                     _log.I("{0} - {1}", column.Name, column.ParquetRawType);
                  }
               }

               // After reading the column types give a printed list of the layout of the columns 
               using (var reader = new Parquet.ParquetReader(fileInfo.Open(System.IO.FileMode.Open)))
               {
                  var dataSet = reader.Read();
                  string columnNames =
                     dataSet.ColumnNames.Aggregate("", (current, columnName) => current + (columnName + "\t"));
                  _log.I(columnNames);
                  for (int i = 0; i < dataSet.Count; i++)
                  {
                     string values = dataSet[i].Aggregate("", (current, value) => current + Convert.ToString(value) + "\t");
                     _log.I(values);
                  }
               }
            }

         }
        }

      private static void WriteHelp()
      {
         _log.I("dotnet parq.dll\t-\tParquet File Inspector for .net");
         _log.I("Usage\t-\tparq.exe InputFilePath=[relativeStringPath]");
      }
   }
}