using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   class TableEnumerator : IEnumerator<Row>
   {
      private readonly Table _table;
      private int _rowIndex = -1;

      public TableEnumerator(Table table)
      {
         _table = table;
      }

      public Row Current => _table.GetRow(_rowIndex);

      object IEnumerator.Current => _table.GetRow(_rowIndex);

      public void Dispose()
      {
         
      }

      public bool MoveNext()
      {
         if (++_rowIndex == _table.Count)
         {
            return false;
         }

         return true;
      }

      public void Reset()
      {
         _rowIndex = -1;
      }
   }
}
