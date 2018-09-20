using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Helps iterating over <see cref="DataColumn"/> returning either a singular value or an array if the column is repeated.
   /// </summary>
   class DataColumnEnumerator : IEnumerator
   {
      private readonly DataColumn _dataColumn;
      private int _position = -1;
      private bool _isArray;

      public DataColumnEnumerator(DataColumn dataColumn)
      {
         _dataColumn = dataColumn;
         _isArray = dataColumn.Field.IsArray;
      }

      public object Current { get; private set; }

      public bool MoveNext()
      {
         if (++_position >= _dataColumn.Data.Length)
            return false;

         if(_isArray)
         {
            var cell = new List<object>();

            while (_position < _dataColumn.Data.Length)
            {
               int rl = _dataColumn.RepetitionLevels[_position];

               if (cell.Count > 0 && rl == 0)
                  break;

               object value = _dataColumn.Data.GetValue(_position);
               cell.Add(value);
               _position += 1;
            }

            Array cellArray = Array.CreateInstance(_dataColumn.Field.ClrNullableIfHasNullsType, cell.Count);
            Array.Copy(cell.ToArray(), cellArray, cell.Count);
            Current = cellArray;
         }
         else
         {
            Current = _dataColumn.Data.GetValue(_position);
         }

         return true;
      }

      public void Reset()
      {
         _position = -1;
      }
   }
}
