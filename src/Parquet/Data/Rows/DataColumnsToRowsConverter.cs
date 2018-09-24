using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   class DataColumnsToRowsConverter
   {
      private readonly Dictionary<string, DataColumnEnumerator> _pathToColumn = new Dictionary<string, DataColumnEnumerator>();
      private readonly Schema _schema;
      private readonly long _totalRowRount;

      public DataColumnsToRowsConverter(Schema schema, DataColumn[] columns, long totalRowRount)
      {
         ValidateColumnsAreInSchema(schema, columns);

         foreach(DataColumn column in columns)
         {
            _pathToColumn[column.Field.Path] = new DataColumnEnumerator(column);
         }

         _schema = schema;
         _totalRowRount = totalRowRount;
      }

      public IReadOnlyCollection<Row> Convert()
      {
         var result = new List<Row>();

         ColumnsToRows(_schema.Fields.ToArray(), result, _totalRowRount);

         return result;
      }

      private void ColumnsToRows(Field[] fields, List<Row> result, long rowCount)
      {
         int rowIndex = 0;

         while (rowCount == -1 || rowIndex < rowCount)
         {
            var row = new List<object>();
            for (int fieldIndex = 0; fieldIndex < fields.Length; fieldIndex++)
            {
               Field f = fields[fieldIndex];

               switch (f.SchemaType)
               {
                  case SchemaType.Data:
                     DataColumnEnumerator dce = _pathToColumn[f.Path];
                     if (!dce.MoveNext())
                     {
                        return;
                     }
                     row.Add(dce.Current);
                     break;

                  case SchemaType.Map:
                     row.Add(CreateMapCell((MapField)f));
                     break;

                  case SchemaType.Struct:
                     //struct is just a row in a cell
                     throw new NotImplementedException();

                  default:
                     throw OtherExtensions.NotImplemented(f.SchemaType.ToString());
               }
            }

            result.Add(new Row(row.ToArray()));

            rowIndex += 1;
         }
      }

      private object CreateMapCell(MapField mapField)
      {
         if (!((mapField.Key is DataField) && (mapField.Value is DataField)))
            throw OtherExtensions.NotImplemented("complex maps");

         var mapRows = new List<Row>();

         DataColumnEnumerator dceKey = _pathToColumn[mapField.Key.Path];
         DataColumnEnumerator dceValue = _pathToColumn[mapField.Value.Path];

         ColumnsToRows(
            new[] { dceKey.DataColumn.Field, dceValue.DataColumn.Field },
            mapRows, -1);

         return mapRows;
      }

      private static void ValidateColumnsAreInSchema(Schema schema, DataColumn[] columns)
      {
         DataField[] schemaFields = schema.GetDataFields();
         DataField[] passedFields = columns.Select(f => f.Field).ToArray();

         if (schemaFields.Length != passedFields.Length)
         {
            throw new ArgumentException($"schema has {schemaFields.Length} fields, but only {passedFields.Length} are passed", nameof(schema));
         }

         for (int i = 0; i < schemaFields.Length; i++)
         {
            DataField sf = schemaFields[i];
            DataField pf = schemaFields[i];

            if (!sf.Equals(pf))
            {
               throw new ArgumentException($"expected {sf} at position {i} but found {pf}");
            }
         }
      }
   }
}
