﻿namespace Parquet.Data
{
   using System;
   using System.Collections;
   using System.Collections.Generic;

   
   static class RowAppender
   {
      public static void Append(
         Dictionary<string, IList> columns,
         IReadOnlyList<Field> schema,
         Row row)
      {
         DataSetValidator.ValidateRow(row, schema);

         for (int i = 0; i < schema.Count; i++)
         {
            Field field = schema[i];
            object value = row[i];

            switch (field.SchemaType)
            {
               case SchemaType.Map:
               {
                  MapField mapField = (MapField)field;

                  IList keys = GetValues(columns, mapField.Key, true, true);
                  IList values = GetValues(columns, mapField.Value, true, true);

                  ((MapField)field).AddElement(keys, values, value as IDictionary);
                  break;
               }
               
               case SchemaType.Struct:
                  AddStructure(columns, field as StructField, value as Row);
                  break;
               
               case SchemaType.List:
                  AddList(columns, field as ListField, value);
                  break;
               
               default:
               {
                  IList values = GetValues(columns, (DataField)field, true);

                  values.Add(value);
                  break;
               }
            }
         }
      }

      static void AddStructure(Dictionary<string, IList> columns, StructField field, Row structRow)
      {
         if (structRow == null)
         {
            throw new ArgumentException($"expected {typeof(Row)} for field [{field}] value");
         }

         Append(columns, field.Fields, structRow);
      }

      static void AddList(Dictionary<string, IList> columns, ListField listField, object value)
      {
         /*
           Value slicing can happen only when entering a list and in no other cases.
           Only list is changing hierarchy dramatically.
          */

         switch(listField.Item.SchemaType)
         {
            case SchemaType.Struct:
               StructField structField = (StructField)listField.Item;
               IEnumerable<Row> rows = value as IEnumerable<Row>;

               var deepColumns = new Dictionary<string, IList>();
               foreach (Row row in rows)
               {
                  Append(deepColumns, structField.Fields, row);
               }
               SliceIn(columns, deepColumns);
               break;

            case SchemaType.Data:
               DataField dataField = (DataField)listField.Item;
               IDataTypeHandler handler = DataTypeFactory.Match(dataField);
               IList values = handler.CreateEmptyList(dataField.HasNulls, dataField.IsArray, 0);
               
               foreach(object v in (IEnumerable)value)
               {
                  values.Add(v);
               }
               GetValues(columns, dataField, true, true).Add(values);
               break;

            default:
               throw OtherExtensions.NotImplementedForPotentialAssholesAndMoaners($"adding {listField.Item.SchemaType} to list");
         }
      }

      static IList GetValues(Dictionary<string, IList> columns, DataField field, bool createIfMissing, bool isNested = false)
      {
         if (field.Path == null) throw new ArgumentNullException(nameof(field.Path));

         if (!columns.TryGetValue(field.Path, out IList values) || values == null)
         {
            if (createIfMissing)
            {
               IDataTypeHandler handler = DataTypeFactory.Match(field);

               values = isNested
                  ? (IList)(new List<IEnumerable>())
                  : (IList)(handler.CreateEmptyList(field.HasNulls, field.IsArray, 0));

               columns[field.Path] = values;
            }
            else
            {
               throw new ArgumentException($"column does not exist by path '{field.Path}'", nameof(field));
            }
         }

         return values;
      }

      static Dictionary<string, IList> SliceIn(Dictionary<string, IList> columns, Dictionary<string, IList> newColumns)
      {
         var result = new Dictionary<string, IList>();

         foreach(KeyValuePair<string, IList> pathAndList in newColumns)
         {
            //ensure list by path exists
            if(!columns.TryGetValue(pathAndList.Key, out IList list))
            {
               list = new List<IList>();
               columns[pathAndList.Key] = list;
            }

            //add new element
            list.Add(pathAndList.Value);
         }

         return result;
      }
   }
}
