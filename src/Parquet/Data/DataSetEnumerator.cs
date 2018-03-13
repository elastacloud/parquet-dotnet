namespace Parquet.Data
{
   using System;
   using System.Collections;
   using System.Collections.Generic;

   
   class DataSetEnumerator :
      IEnumerator<Row>
   {
      int _rowIndex = -1;
      readonly DataSet _ds;

      public DataSetEnumerator(DataSet ds)
      {
         _ds = ds ?? throw new ArgumentNullException(nameof(ds));
      }

      public Row Current => _ds[_rowIndex];

      object IEnumerator.Current => _ds[_rowIndex];

      public void Dispose()
      {
      }

      public bool MoveNext()
      {
         if (++_rowIndex == _ds.RowCount) return false;

         return true;
      }

      public void Reset()
      {
         _rowIndex = -1;
      }
   }
}
