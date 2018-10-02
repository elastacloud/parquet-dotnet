using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data.Rows
{
   class DataColumnAppender
   {
      private readonly DataField _dataField;
      private readonly List<object> _values = new List<object>();
      private readonly List<int> _rls = new List<int>();
      private readonly bool _isRepeated;

      public DataColumnAppender(DataField dataField)
      {
         _dataField = dataField;
         _isRepeated = dataField.MaxRepetitionLevel > 0;
      }

      public void Add(object value)
      {
         if (_isRepeated)
         {
            int rl = 0;
            foreach(object valueItem in (IEnumerable)value)
            {
               _values.Add(valueItem);
               _rls.Add(rl);
               rl = _dataField.MaxRepetitionLevel;
            }
         }
         else
         {
            _values.Add(value);
         }
      }

      public DataColumn ToDataColumn()
      {
         Array data = Array.CreateInstance(_dataField.ClrNullableIfHasNullsType, _values.Count);

         for(int i = 0; i < _values.Count; i++)
         {
            data.SetValue(_values[i], i);
         }

         return new DataColumn(_dataField, data, _isRepeated ? _rls.ToArray() : null);
      }

      public override string ToString() => _dataField.ToString();
   }
}