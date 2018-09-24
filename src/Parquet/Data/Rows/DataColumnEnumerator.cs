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
      private int _position = -1;
      private readonly bool _isRepeated;
      private readonly Array _data;
      private readonly int[] _rls;
      private readonly DataField _field;
      private readonly DataColumn _dc;

      public DataColumnEnumerator(DataColumn dataColumn)
      {
         //_isRepeated = dataColumn.HasRepetitions;
         _isRepeated = dataColumn.Field.IsArray;
         _data = dataColumn.Data;
         _rls = dataColumn.RepetitionLevels;
         _field = dataColumn.Field;
         _dc = dataColumn;
      }

      public object Current { get; private set; }

      public DataColumn DataColumn => _dc;

      public bool MoveNext()
      {
         if (++_position >= _data.Length)
            return false;

         if(_isRepeated)
         {
            var cell = new List<object>();

            while (_position < _data.Length)
            {
               int rl = _rls[_position];

               if (cell.Count > 0 && rl == 0)
               {
                  _position -= 1;   //rewind back as this doesn't belong to you
                  break;
               }

               object value = _data.GetValue(_position);
               cell.Add(value);
               _position += 1;
            }

            Array cellArray = Array.CreateInstance(_field.ClrNullableIfHasNullsType, cell.Count);
            Array.Copy(cell.ToArray(), cellArray, cell.Count);
            Current = cellArray;
         }
         else
         {
            Current = _data.GetValue(_position);
         }

         return true;
      }

      public void Reset()
      {
         _position = -1;
      }

      public override string ToString()
      {
         return $"{_position}/{_data.Length} of {_field}";
      }
   }
}
