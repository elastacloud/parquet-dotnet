using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data.Rows
{
   internal static class RowMatrix
   {
      #region [ Validation ]

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
                  ValidatePrimitive((DataField)field, value);
                  break;
               case SchemaType.Map:
                  ValidateMap((MapField)field, value);
                  break;
               default:
                  throw new NotImplementedException(field.SchemaType.ToString());
            }
         }
      }

      private static void ValidateMap(MapField mf, object value)
      {
         DataField keyField = (DataField)mf.Key;
         DataField valueField = (DataField)mf.Value;

         if(!value.GetType().TryExtractDictionaryType(out Type keyType, out Type valueType) ||
            keyType != keyField.ClrType || valueType != valueField.ClrType)
         {
            throw new ArgumentException($"expected dictionary of {keyField.ClrType}:{valueField.ClrType} but found {value.GetType()}");
         }

         IDictionary dictionary = (IDictionary)value;
         foreach(object dkey in dictionary.Keys)
         {
            ValidatePrimitive(keyField, dkey);
         }
         foreach(object dvalue in dictionary.Values)
         {
            ValidatePrimitive(valueField, dvalue);
         }
      }

      private static void ValidatePrimitive(DataField df, object value)
      {
         if(value == null)
         {
            if (!df.HasNulls)
               throw new ArgumentException($"element is null but column '{df.Name}' does not accept nulls");
         }
         else
         {
            Type vt = value.GetType();
            Type st = df.ClrType;

            if (vt.IsNullable())
               vt = vt.GetNonNullable();

            if(df.IsArray)
            {
               if(!vt.IsArray)
               {
                  throw new ArgumentException($"expected array but found {vt}");
               }

               if(vt.GetElementType() != st)
               {
                  throw new ArgumentException($"expected array element type {st} but found {vt.GetElementType()}");
               }
            }
            else
            {
               if (vt != st)
                  throw new ArgumentException($"expected {st} but found {vt}");
            }
         }
      }

      #endregion

      #region [ Table/Row packing ]

      public static DataColumn[] Extract(Schema schema, IReadOnlyCollection<Row> rows)
      {
         var dcs = new List<DataColumn>();

         Extract(schema.Fields, rows, dcs);

         return dcs.ToArray();
      }

      private static void Extract(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row> rows, List<DataColumn> dcs)
      {
         int i = 0;
         foreach(Field field in fields)
         {
            switch (field.SchemaType)
            {
               case SchemaType.Data:
                  dcs.Add(
                     Extract(
                        (DataField)field,
                        rows.Select(r => r[i]),
                        rows.Count));
                  break;
            }

            i += 1;
         }
      }

      private static DataColumn Extract(DataField dataField, IEnumerable<object> values, int? length)
      {
         if(dataField.IsArray)
         {
            //don't know beforehand how many elements are in the target array, expand the array first!
            var flatValues = new List<object>();
            var repetitionLevels = new List<int>();
            foreach(Array valueArray in values)
            {
               int rl = 0;
               foreach(object value in valueArray)
               {
                  repetitionLevels.Add(rl);
                  flatValues.Add(value);
                  rl = 1;
               }
            }

            //allocate flat array for data column
            Array data = Array.CreateInstance(dataField.ClrNullableIfHasNullsType, flatValues.Count);
            for(int i = 0; i < flatValues.Count; i++)
            {
               data.SetValue(flatValues[i], i);
            }

            //return data column with valid repetition levels
            return new DataColumn(dataField, data, repetitionLevels.ToArray());
         }
         else
         {
            Array data = Array.CreateInstance(dataField.ClrNullableIfHasNullsType, length.Value);
            int i = 0;
            foreach(object value in values)
            {
               data.SetValue(value, i++);
            }

            return new DataColumn(dataField, data);
         }
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
         int[] walkIndexes = new int[columns.Length];

         for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
         {
            int dataColumnIndex = 0;
            var row = new List<object>();
            for (int fieldIndex = 0; fieldIndex < fields.Length; fieldIndex++)
            {
               Field f = fields[fieldIndex];

               switch (f.SchemaType)
               {
                  case SchemaType.Data:
                     int walkIndex = walkIndexes[dataColumnIndex];
                     row.Add(CompactDataCell((DataField)f, columns[dataColumnIndex], rowIndex, ref walkIndex));
                     walkIndexes[dataColumnIndex] = walkIndex;
                     dataColumnIndex += 1;
                     break;
                  default:
                     throw new NotImplementedException(f.SchemaType.ToString());
               }
            }

            container.Add(new Row(row.ToArray()));
         }
      }

      private static object CompactDataCell(DataField dataField, DataColumn dataColumn, int rowIndex, ref int walkIndex)
      {
         if (dataField.IsArray)
         {
            var cell = new List<object>();

            while (walkIndex < dataColumn.Data.Length)
            {
               int rl = dataColumn.RepetitionLevels[walkIndex];

               if (cell.Count > 0 && rl == 0)
                  break;

               object value = dataColumn.Data.GetValue(walkIndex);
               cell.Add(value);
               walkIndex += 1;
            }

            Array cellArray = Array.CreateInstance(dataField.ClrNullableIfHasNullsType, cell.Count);
            Array.Copy(cell.ToArray(), cellArray, cell.Count);
            return cellArray;
         }
         else
         {
            return dataColumn.Data.GetValue(rowIndex);
         }
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