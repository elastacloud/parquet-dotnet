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

         ColumnsToRows(_schema.Fields, result, _totalRowRount);

         return result;
      }

      private void ColumnsToRows(IEnumerable<Field> fields, List<Row> result, long rowCount)
      {
         for(int rowIndex = 0; rowCount == -1 || rowIndex < rowCount; rowIndex++)
         {
            Row row = BuildNextRow(fields);

            if (row == null)
               return;

            result.Add(row);
         }
      }

      private Row BuildNextRow(IEnumerable<Field> fields)
      {
         var row = new List<object>();
         foreach(Field f in fields)
         {
            object cell;

            switch (f.SchemaType)
            {
               case SchemaType.Data:
                  DataColumnEnumerator dce = _pathToColumn[f.Path];
                  if (!dce.MoveNext())
                  {
                     return null;
                  }
                  cell = dce.Current;
                  break;

               case SchemaType.Map:
                  cell = CreateMapCell((MapField)f);
                  break;

               case SchemaType.Struct:
                  cell = CreateStructCell((StructField)f);
                  break;

               case SchemaType.List:
                  cell = CreateListCell((ListField)f);
                  break;

               default:
                  throw OtherExtensions.NotImplemented(f.SchemaType.ToString());
            }

            if(cell == null)
            {
               return null;
            }

            row.Add(cell);
         }

         return new Row(row);
      }

      private object CreateListCell(ListField lf)
      {
         var rows = new List<Row>();
         var fields = new Field[] { lf.Item };
         Row row;
         while((row = BuildNextRow(fields)) != null)
         {
            rows.Add(row);
         }

         if(lf.Item.SchemaType == SchemaType.Data)
         {
            //remap from first column
            return rows.Select(r => r[0]).ToArray();
         }

         return rows;
      }

      private object CreateStructCell(StructField sf)
      {
         Row row = BuildNextRow(sf.Fields);

         return row;
      }

      private object CreateMapCell(MapField mf)
      {
         if (!((mf.Key is DataField) && (mf.Value is DataField)))
            throw OtherExtensions.NotImplemented("complex maps");

         var mapRows = new List<Row>();

         DataColumnEnumerator dceKey = _pathToColumn[mf.Key.Path];
         DataColumnEnumerator dceValue = _pathToColumn[mf.Value.Path];

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
