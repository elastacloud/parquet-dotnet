using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Navigates the table
   /// </summary>
   public abstract class TableWalker
   {
      private readonly Table _table;

      /// <summary>
      /// 
      /// </summary>
      protected TableWalker(Table table)
      {
         _table = table;
      }

      /// <summary>
      /// Starts the walk
      /// </summary>
      public void Walk()
      {
         OpenTable();

         Walk(null, _table.Schema.Fields.ToList(), _table.ToList(), 0);

         CloseTable();
      }

      private void Walk(Field parent, List<Field> fields, List<Row> rows, int level)
      {
         for(int i = 0; i < rows.Count; i++)
         {
            Walk(parent, fields, rows[i], level, i == 0, i == rows.Count - 1);
         }
      }

      private void Walk(Field parent, List<Field> fields, Row row, int level, bool isFirst, bool isLast)
      {
         OpenRow(parent, row, level, isFirst, isLast);

         for(int i = 0; i < fields.Count; i++)
         {
            WalkRowValue(fields[i], row[i], level, i == 0, i == fields.Count - 1);
         }

         CloseRow(parent, row, level, isFirst, isLast);
      }

      private void WalkRowValue(Field f, object value, int level, bool isFirst, bool isLast)
      {
         if(value == null)
         {
            OpenValue(f, null, level, isFirst, isLast);
         }
         else if(value is Row row)
         {
            switch (f.SchemaType)
            {
               case SchemaType.Struct:
                  Walk(f, ((StructField)f).Fields.ToList(), row, level + 1, true, true);
                  break;
               default:
                  throw new NotImplementedException(f.ToString());
            }
         }
         else if((!value.GetType().IsSimple()) && value is IEnumerable ien)
         {
            List<object> ar = ien.Cast<object>().ToList();

            for(int i = 0; i < ar.Count; i++)
            {
               WalkRowValue(null, ar[i], level + 1, i == 0, i == ar.Count - 1);
            }
         }
         else
         {
            OpenValue(f, value, level, isFirst, isLast);
         }
      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual void OpenValue(Field f, object value, int level, bool isFirst, bool isLast)
      {

      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual void OpenRow(Field f, Row row, int level, bool isFirst, bool isLast)
      {

      }

      /// <summary>
      /// 
      /// </summary>
      protected virtual void CloseRow(Field f, Row row, int level, bool isFirst, bool isLast)
      {

      }

      /// <summary>
      /// Table opened
      /// </summary>
      protected virtual void OpenTable()
      {

      }

      /// <summary>
      /// Table closed
      /// </summary>
      protected virtual void CloseTable()
      {

      }


   }
}
