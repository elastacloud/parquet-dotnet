using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Defines extension methods to simplify Parquet usage (experimental v3)
   /// </summary>
   public static class ParquetExtensions
   {
      /// <summary>
      /// Writes a file with a single row group
      /// </summary>
      public static void WriteSingleRowGroupParquetFile(this Stream stream, Schema schema, int rowCount, params DataColumn[] columns)
      {
         using (var writer = new ParquetWriter(schema, stream))
         {
            writer.CompressionMethod = CompressionMethod.None;
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup(rowCount))
            {
               foreach(DataColumn column in columns)
               {
                  rgw.WriteColumn(column);
               }
            }
         }
      }

      /// <summary>
      /// Reads the first row group from a file
      /// </summary>
      /// <param name="stream"></param>
      /// <param name="schema"></param>
      /// <param name="columns"></param>
      public static void ReadSingleRowGroupParquetFile(this Stream stream, out Schema schema, out DataColumn[] columns)
      {
         using (var reader = new ParquetReader(stream))
         {
            schema = reader.Schema;

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               DataField[] dataFields = schema.GetDataFields();
               columns = new DataColumn[dataFields.Length];

               for(int i = 0; i < dataFields.Length; i++)
               {
                  columns[i] = rgr.ReadColumn(dataFields[i]);
               }
            }
         }
      }

      /// <summary>
      /// Writes entire table in a single row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static void Write(this ParquetWriter writer, Table table)
      {
         using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup(table.Count))
         {
            rowGroupWriter.Write(table);
         }
      }

      /// <summary>
      /// Reads the first row group as a table
      /// </summary>
      /// <param name="reader">Open reader</param>
      /// <returns></returns>
      public static Table ReadAsTable(this ParquetReader reader)
      {
         if (reader.RowGroupCount > 1)
         {
            throw new NotImplementedException("current implementation supports only single row group files");
         }

         using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
         {
            DataColumn[] allData = reader.Schema.GetDataFields().Select(df => rowGroupReader.ReadColumn(df)).ToArray();

            return new Table(reader.Schema, allData, rowGroupReader.RowCount);
         }
      }

      /// <summary>
      /// Writes table to this row group
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="table"></param>
      public static void Write(this ParquetRowGroupWriter writer, Table table)
      {
         foreach (DataField dataField in table.Schema.GetDataFields())
         {
            DataColumn dc = table.ExtractDataColumn(dataField);

            writer.WriteColumn(dc);
         }
      }
   }
}