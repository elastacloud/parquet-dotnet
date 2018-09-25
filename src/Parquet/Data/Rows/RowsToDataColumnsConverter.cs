using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   class RowsToDataColumnsConverter
   {
      private readonly Schema _schema;
      private readonly IReadOnlyCollection<Row> _rows;

      public RowsToDataColumnsConverter(Schema schema, IReadOnlyCollection<Row> rows)
      {
         _schema = schema;
         _rows = rows;
      }

      public IReadOnlyCollection<DataColumn> Convert()
      {
         return RowsToColumns(_schema.Fields, _rows);
      }

      private static DataColumn[] RowsToColumns(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row> rows)
      {
         var dcs = new List<DataColumn>();

         int i = 0;
         foreach (Field f in fields)
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

         foreach (IReadOnlyCollection<Row> rows in values)
         {
            DataColumn[] rowsColumns = RowsToColumns(new Field[] { mapField.Key, mapField.Value }, rows);

            //add correct repetition levels
            rowsColumns = rowsColumns.Select(rc => new DataColumn(rc.Field, rc.Data, rc.Field.GenerateRepetitions(rc.Data.Length))).ToArray();

            if (columns == null)
            {
               columns = rowsColumns;
            }
            else
            {
               //columns = rowsColumns.Select((dc, i) => )
               throw new NotImplementedException();
            }

         }

         foreach (DataColumn column in columns)
         {
            resultColumns.Add(column);
         }
      }

      private static DataColumn RowToDataColumn(DataField dataField, IReadOnlyCollection<object> values)
      {
         if (dataField.IsArray)
         {
            //don't know beforehand how many elements are in the target array, expand the array first!
            var flatValues = new List<object>();
            var repetitionLevels = new List<int>();
            foreach (Array valueArray in values)
            {
               int rl = 0;
               foreach (object value in valueArray)
               {
                  repetitionLevels.Add(rl);
                  flatValues.Add(value);
                  rl = 1;
               }
            }

            //allocate flat array for data column
            Array data = Array.CreateInstance(dataField.ClrNullableIfHasNullsType, flatValues.Count);
            for (int i = 0; i < flatValues.Count; i++)
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
            foreach (object value in values)
            {
               data.SetValue(value, i++);
            }

            return new DataColumn(dataField, data);
         }
      }
   }
}
