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
      private readonly Dictionary<string, DataColumnAppender> _pathToDataColumn = new Dictionary<string, DataColumnAppender>();

      public RowsToDataColumnsConverter(Schema schema, IReadOnlyCollection<Row> rows)
      {
         _schema = schema;
         _rows = rows;
      }

      public IReadOnlyCollection<DataColumn> Convert()
      {
         ProcessRows(_schema.Fields, _rows);

         List<DataColumn> result = _schema.GetDataFields()
            .Select(df => _pathToDataColumn[df.Path].ToDataColumn())
            .ToList();

         return result;
      }

      private void ProcessRows(IReadOnlyCollection<Field> fields, IReadOnlyCollection<Row> rows)
      {
         foreach(Row row in rows)
         {
            ProcessRow(fields, row);
         }
      }

      private void ProcessRow(IReadOnlyCollection<Field> fields, Row row)
      {
         int cellIndex = 0;
         foreach(Field f in fields)
         {
            switch (f.SchemaType)
            {
               case SchemaType.Data:
                  ProcessDataValue(f, row[cellIndex]);
                  break;

               case SchemaType.Map:
                  ProcessMap((MapField)f, (IReadOnlyCollection<Row>)row[cellIndex]);
                  break;

               case SchemaType.Struct:
                  ProcessRow(((StructField)f).Fields, (Row)row[cellIndex]);
                  break;

               case SchemaType.List:
                  ProcessList((ListField)f, row[cellIndex]);
                  break;

               default:
                  throw new NotImplementedException();
            }

            cellIndex++;
         }
      }

      private void ProcessMap(MapField mapField, IReadOnlyCollection<Row> mapRows)
      {
         var fields = new Field[] { mapField.Key, mapField.Value };

         var keyCell = mapRows.Select(r => r[0]).ToList();
         var valueCell = mapRows.Select(r => r[1]).ToList();
         var row = new Row(keyCell, valueCell);

         ProcessRow(fields, row);
      }

      private void ProcessList(ListField listField, object cellValue)
      {
         Field f = listField.Item;

         
         if(f.SchemaType == SchemaType.Data)
         {
            //list has a special case for simple elements where they are not wrapped in rows
            ProcessDataValue(f, cellValue);
         }
         else
         {
            //otherwise it's a collection of rows
            ProcessRows(new[] { f }, (IReadOnlyCollection<Row>)cellValue);
         }
      }

      private void ProcessDataValue(Field f, object value)
      {
         //prepare value appender
         if(!_pathToDataColumn.TryGetValue(f.Path, out DataColumnAppender appender))
         {
            appender = new DataColumnAppender((DataField)f);
            _pathToDataColumn[f.Path] = appender;
         }

         appender.Add(value);
      }
   }
}
