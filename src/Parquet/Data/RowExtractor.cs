﻿namespace Parquet.Data
{
   using System;
   using System.Collections;
   using System.Collections.Generic;
   using System.Linq;
   

   static class RowExtractor
   {
      public static Row Extract(IEnumerable<Field> fields, int index, Dictionary<string, IList> columns)
      {
         return new Row(fields.Select(se => CreateElement(se, index, columns)));
      }

      static object CreateElement(Field field, int index, Dictionary<string, IList> columns)
      {
         switch (field.SchemaType)
         {
            case SchemaType.Map:
               return ((MapField)field).CreateCellValue(columns, index);
            
            case SchemaType.Struct:
               return Extract(((StructField)field).Fields, index, columns);
            
            case SchemaType.List:
               ListField lf = (ListField)field;

               switch (lf.Item.SchemaType)
               {
                  case SchemaType.Struct:
                     StructField structField = (StructField)lf.Item;
                     Dictionary<string, IList> elementColumns = CreateFieldColumns(structField.Fields, index, columns, out int count);

                     var rows = new List<Row>(count);
                     for (int i = 0; i < count; i++)
                     {
                        Row row = Extract(structField.Fields, i, elementColumns);
                        rows.Add(row);
                     }

                     return rows;
                  
                  case SchemaType.Data:
                     DataField dataField = (DataField)lf.Item;
                     IList fieldPathValues = GetFieldPathValues(dataField, index, columns);
                     
                     return fieldPathValues;
                  
                  default:
                     throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners($"reading {lf.Item.SchemaType} from lists");
               }
            
            default:
               if (!columns.TryGetValue(field.Path, out IList values))
               {
                  throw new ParquetException($"something terrible happened, there is no column by name '{field.Name}' and path '{field.Path}'");
               }

               return values[index];
         }
      }

      static Dictionary<string, IList> CreateFieldColumns(
         IEnumerable<Field> fields, int index,
         Dictionary<string, IList> columns,
         out int count)
      {
         var elementColumns = new Dictionary<string, IList>();

         count = int.MaxValue;

         foreach (Field field in fields)
         {
            string key = field.Path;

            switch (field.SchemaType)
            {
               case SchemaType.Data:
                  IList value = columns[key][index] as IList;
                  elementColumns[key] = value;
                  if (value.Count < count) count = value.Count;
                  break;

               case SchemaType.List:
                  var listField = (ListField)field;
                  Dictionary<string, IList> listColumns = CreateFieldColumns(new[] { listField.Item }, index, columns, out int listCount);
                  elementColumns.AddRange(listColumns);
                  count = Math.Min(count, listColumns.Min(kvp => kvp.Value.Count));
                  break;

               case SchemaType.Struct:
                  var structField = (StructField)field;
                  Dictionary<string, IList> structColumns = CreateFieldColumns(structField.Fields, index, columns, out int structCount);
                  elementColumns.AddRange(structColumns);
                  count = Math.Min(count, structColumns.Min(kvp => kvp.Value.Count));
                  break;

               default:
                  throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners($"extracting {field.SchemaType} columns");
            }
         }

         return elementColumns;
      }

      static IList GetFieldPathValues(Field field, int index, Dictionary<string, IList> columns)
      {
         IList values = columns[field.Path][index] as IList;
         return values;
      }
   }
}
