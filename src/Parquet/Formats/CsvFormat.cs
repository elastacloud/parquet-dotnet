using NetBox.FileFormats;
using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Parquet.Formats
{
   /// <summary>
   /// CSV support for DataSet
   /// </summary>
   public static class CsvFormat
   {
      public static void ReadCsv(this DataSet ds, Stream csvStream, CsvOptions options = null)
      {
         if (options == null) options = new CsvOptions();

         var reader = new CsvReader(csvStream, Encoding.UTF8);

         string[] headers = null;
         var columnValues = new Dictionary<int, IList>();

         string[] values;
         while((values = reader.ReadNextRow()) != null)
         {
            //set headers
            if(headers == null)
            {
               if(options.HasHeaders)
               {
                  headers = values;
                  continue;
               }
               else
               {
                  headers = new string[values.Length];
                  for(int i = 0; i < values.Length; i++)
                  {
                     headers[i] = $"Col{i}";
                  }
               }
            }

            //get values
            for(int i = 0; i < values.Length; i++)
            {
               if(!columnValues.TryGetValue(i, out IList col))
               {
                  col = new List<string>();
                  columnValues[i] = col;
               }

               col.Add(values[i]);
            }
         }

         //put back in DS
         Type[] columnTypes = new Type[headers.Length];

         if (options.InferSchema)
         {
            for(int i = 0; i < headers.Length; i++)
            {
               IList cv = columnValues[i];
               Type columnType = cv.Cast<string>().ToArray().InferType(out IList typedValues);
               columnTypes[i] = columnType;
               columnValues[i] = typedValues;
            }
         }
         else
         {
            for(int i = 0; i < headers.Length; i++)
            {
               columnTypes[i] = typeof(string);
            }
         }

         //todo: continue, collect all together

      }
   }
}
