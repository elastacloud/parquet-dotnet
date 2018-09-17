using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data.Rows
{
   internal static class RowMatrix
   {
      public static void Validate(Row row, IReadOnlyList<Field> fields)
      {
         for (int i = 0; i < fields.Count; i++)
         {
            Field field = fields[i];
            object value = row[i];
            Type vt = value == null ? null : value.GetType();

            switch (field.SchemaType)
            {
               case SchemaType.Data:
                  DataField df = (DataField)field;

                  if(value == null && !df.HasNulls)
                  {
                     throw new ArgumentException($"no nulls allowed in {df}");
                  }

                  if(vt != df.ClrType)
                  {
                     throw new ArgumentException($"expected {df.ClrType} but found {vt} in {df}");
                  }

                  break;
               default:
                  throw new NotImplementedException(field.SchemaType.ToString());
            }
         }
      }

      #region [ Table/Row packing ]

      public static DataColumn Extract(Table table, DataField dataField)
      {
         int fieldIndex = table.Schema.GetFieldIndex(dataField);

         var valueList = new List<object>();

         foreach(Row row in table)
         {
            valueList.Add(row[fieldIndex]);
         }

         IDataTypeHandler handler = DataTypeFactory.Match(dataField);
         Array columnData = handler.GetArray(valueList.Count, false, dataField.HasNulls);
         
         for(int i = 0; i < valueList.Count; i++)
         {
            columnData.SetValue(valueList[i], i);
         }

         return new DataColumn(dataField, columnData);
      }

      public static List<Row> Compact(Schema schema, DataColumn[] columns, long rowCount)
      {
         ValidateColumnsAreInSchema(schema, columns);

         var result = new List<Row>();

         Compact(schema.Fields.ToArray(), columns, result, rowCount);

         return result;
      }

      private static void Compact(Field[] fields, DataColumn[] columns, List<Row> container, long rowCount)
      {
         for (int ri = 0; ri < rowCount; ri++)
         {
            int ci = 0;
            var row = new List<object>();
            for (int fi = 0; fi < fields.Length; fi++)
            {
               Field f = fields[fi];

               switch (f.SchemaType)
               {
                  case SchemaType.Data:
                     row.Add(CompactDataCell((DataField)f, columns[ci++], ri));
                     break;
                  default:
                     throw new NotImplementedException(f.SchemaType.ToString());
               }
            }

            container.Add(new Row(row.ToArray()));
         }
      }

      private static object CompactDataCell(DataField dataField, DataColumn dataColumn, int rowIndex)
      {
         object value = dataColumn.Data.GetValue(rowIndex);

         return value;
      }

      private static void ValidateColumnsAreInSchema(Schema schema, DataColumn[] columns)
      {
         DataField[] schemaFields = schema.GetDataFields();
         DataField[] passedFields = columns.Select(f => f.Field).ToArray();

         if(schemaFields.Length != passedFields.Length)
         {
            throw new ArgumentException($"schema has {schemaFields.Length} fields, but only {passedFields.Length} are passed", nameof(schema));
         }

         for(int i = 0; i < schemaFields.Length; i++)
         {
            DataField sf = schemaFields[i];
            DataField pf = schemaFields[i];

            if(!sf.Equals(pf))
            {
               throw new ArgumentException($"expected {sf} at position {i} but found {pf}");
            }
         }
      }

      #endregion

   }
}