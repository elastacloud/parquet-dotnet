using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   internal static class RowMatrix
   {
      public static void Validate(Row row)
      {

      }


      public static void Append(Table table, Row row)
      {
         Append(table, table.Schema.Fields, row);
      }

      public static Row Extract(Table table, int index)
      {
         return Extract(table, table.Schema.Fields, index);
      }

      #region [ Table/Row packing ]

      private static void Append(Table table, IReadOnlyList<Field> fields, Row row)
      {
         for (int i = 0; i < fields.Count; i++)
         {
            Field field = fields[i];
            object value = row[i];

            switch (field.SchemaType)
            {
               case SchemaType.Data:
                  IList column = FindTargetList(table, (DataField)field);
                  column.Add(value);
                  break;
               default:
                  throw new NotImplementedException(field.SchemaType.ToString());
            }
         }
      }

      private static Row Extract(Table table, IReadOnlyList<Field> fields, int index)
      {
         return new Row(fields.Select(f => ExtractCell(table, f, index, table._fieldPathToValues)).ToArray());
      }

      private static object ExtractCell(Table table, Field field, int index, Dictionary<string, IList> pathToValues)
      {
         switch (field.SchemaType)
         {
            case SchemaType.Data:
               if(!pathToValues.TryGetValue(field.Path, out IList values))
               {
                  throw new NotImplementedException();
               }
               return values[index];
            default:
               throw new NotImplementedException(field.SchemaType.ToString());
         }
      }

      private static IList FindTargetList(Table table, DataField dataField)
      {
         if(!table._fieldPathToValues.TryGetValue(dataField.Path, out IList values))
         {
            values = new List<object>();
            table._fieldPathToValues[dataField.Path] = values;
         }

         return values;
      }

      #endregion

   }
}