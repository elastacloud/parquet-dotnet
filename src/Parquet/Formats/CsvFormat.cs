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
      private static readonly Dictionary<Type, Type> InferredTypeToParquetType = new Dictionary<Type, Type>
      {
         { typeof(byte), typeof(int) }
      };
      
      public static DataSet ReadToDataSet(Stream csvStream, CsvOptions options = null)
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

         DataSet result;

         //set schema
         if (options.InferSchema)
         {
            result = InferSchema(headers, columnValues);
         }
         else
         {
            throw new NotImplementedException("infer schema must be set to true");
         }

         //assign values
         result.AddColumnar(columnValues.Values);
         return result;
      }

      private static DataSet InferSchema(string[] headers, Dictionary<int, IList> columnValues)
      {
         var elements = new List<SchemaElement>();
         for (int i = 0; i < headers.Length; i++)
         {
            IList cv = columnValues[i];
            Type columnType = cv.Cast<string>().ToArray().InferType(out IList typedValues);

            Type ct;
            if (!InferredTypeToParquetType.TryGetValue(columnType, out ct)) ct = columnType;
            elements.Add(new SchemaElement(headers[i], ct));

            columnValues[i] = typedValues;
         }

         return new DataSet(elements);
      }
   }
}
