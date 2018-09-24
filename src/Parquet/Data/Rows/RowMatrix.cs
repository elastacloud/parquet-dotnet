using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Everything is rows!!! Not dealing with dictionaries etc. seems like a brilliant idea!!!
   /// </summary>
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
         if(!value.GetType().TryExtractEnumerableType(out Type elementType))
         {
            throw new ArgumentException($"map must be a collection of rows");
         }

         if(elementType != typeof(Row))
         {
            throw new ArgumentException($"map must be a collection of rows, but found a collection of {elementType}");
         }

         foreach(Row row in (IEnumerable)value)
         {
            Validate(row, new[] { mf.Key, mf.Value });
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

      #region [ Conversion: Rows => DataColumns ]

      public static DataColumn[] RowsToColumns(Schema schema, IReadOnlyCollection<Row> rows)
      {
         return RowsToColumns(schema.Fields.ToArray(), rows);
      }

      private static DataColumn[] RowsToColumns(Field[] fields, IReadOnlyCollection<Row> rows)
      {
         var dcs = new List<DataColumn>();

         int i = 0;
         foreach(Field f in fields)
         {
            List<object> values = rows.Select(r => r[i]).ToList();

            RowsToColumn(f, values, dcs);

            i += 1;
         }

         return dcs.ToArray();
      }

      private static void RowsToColumn(Field field, IReadOnlyCollection<object> values, IList<DataColumn> result)
      {
         switch (field.SchemaType)
         {
            case SchemaType.Data:
               //always maps to a single column
               result.Add(RowToDataColumn((DataField)field, values));
               break;
            case SchemaType.Map:
               RowToColumns((MapField)field, values, result);
               break;
            default:
               throw new NotImplementedException(field.SchemaType.ToString());
         }
      }

      private static void RowToColumns(MapField mapField, IEnumerable<object> values, IList<DataColumn> resultColumns)
      {
         DataField keyField = (DataField)mapField.Key;
         DataField valueField = (DataField)mapField.Value;

         //values are instances of Row collections

         DataColumn[] columns = null;

         foreach(IReadOnlyCollection<Row> rows in values)
         {
            DataColumn[] rowsColumns = RowsToColumns(new Field[] { mapField.Key, mapField.Value }, rows);

            //add correct repetition levels
            rowsColumns = rowsColumns.Select(rc => new DataColumn(rc.Field, rc.Data, rc.Field.GenerateRepetitions(rc.Data.Length))).ToArray();
            
            if(columns == null)
            {
               columns = rowsColumns;
            }
            else
            {
               //columns = rowsColumns.Select((dc, i) => )
               throw new NotImplementedException();
            }

         }

         foreach(DataColumn column in columns)
         {
            resultColumns.Add(column);
         }
      }

      private static DataColumn RowToDataColumn(DataField dataField, IReadOnlyCollection<object> values)
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
            Array data = Array.CreateInstance(dataField.ClrNullableIfHasNullsType, values.Count);
            int i = 0;
            foreach(object value in values)
            {
               data.SetValue(value, i++);
            }

            return new DataColumn(dataField, data);
         }
      }

      #endregion

      #region [ Conversion: DataColumns => Rows ]

      public static List<Row> ColumnsToRows(Schema schema, DataColumn[] columns, long rowCount)
      {
         ValidateColumnsAreInSchema(schema, columns);

         var result = new List<Row>();

         DataColumnEnumerator[] iterated = columns.Select(c => new DataColumnEnumerator(c)).ToArray();

         ColumnsToRows(schema.Fields.ToArray(), iterated, result, rowCount);

         return result;
      }

      private static void ColumnsToRows(Field[] fields, DataColumnEnumerator[] columns, List<Row> result, long rowCount)
      {
         int rowIndex = 0;

         while(rowCount == -1 || rowIndex < rowCount)
         {
            int dataColumnIndex = 0;
            var row = new List<object>();
            for (int fieldIndex = 0; fieldIndex < fields.Length; fieldIndex++)
            {
               Field f = fields[fieldIndex];

               switch (f.SchemaType)
               {
                  case SchemaType.Data:
                     DataColumnEnumerator dce = columns[dataColumnIndex];
                     if(!dce.MoveNext())
                     {
                        return;
                     }
                     row.Add(dce.Current);
                     dataColumnIndex += 1;
                     break;

                  case SchemaType.Map:
                     row.Add(CreateMapCell((MapField)f, columns, ref dataColumnIndex));
                     break;

                  default:
                     throw OtherExtensions.NotImplemented(f.SchemaType.ToString());
               }
            }

            result.Add(new Row(row.ToArray()));

            rowIndex += 1;
         }
      }

      private static object CreateMapCell(MapField mapField, DataColumnEnumerator[] columns, ref int dataColumnIndex)
      {
         if (!((mapField.Key is DataField) && (mapField.Value is DataField)))
            throw OtherExtensions.NotImplemented("complex maps");

         var mapRows = new List<Row>();

         DataColumnEnumerator dceKey = columns[dataColumnIndex];
         DataColumnEnumerator dceValue = columns[dataColumnIndex + 1];

         ColumnsToRows(
            new[] { dceKey.DataColumn.Field, dceValue.DataColumn.Field },
            new[] { dceKey, dceValue },
            mapRows, -1);

         dataColumnIndex += 2;

         return mapRows;
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