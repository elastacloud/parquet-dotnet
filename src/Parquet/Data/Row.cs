using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace Parquet.Data
{
   /// <summary>
   /// Represents a row
   /// </summary>
   public class Row
   {
      private object[] _values;

      public Row(IEnumerable<object> values)
      {
         _values = values.ToArray();
      }

      public int Length => _values.Length;

      public object this[int i]
      {
         get
         {
            return _values[i];
         }
      }

      public bool GetBoolean(int i)
      {
         return Get<bool>(i);
      }

      public int GetInt(int i)
      {
         return Get<int>(i);
      }

      public float GetFloat(int i)
      {
         return Get<float>(i);
      }

      public long GetLong(int i)
      {
         return Get<long>(i);
      }

      public double GetDouble(int i)
      {
         return Get<double>(i);
      }

      public BigInteger GetBigInt(int i)
      {
         return Get<BigInteger>(i);
      }

      public byte[] GetByteArray(int i)
      {
         return Get<byte[]>(i);
      }

      public string GetString(int i)
      {
         return Get<string>(i);
      }

      public DateTimeOffset GetDateTimeOffset(int i)
      {
         return Get<DateTimeOffset>(i);
      }

      /// <summary>
      /// Returns true if value at column <paramref name="i"/> is NULL.
      /// </summary>
      public bool IsNullAt(int i)
      {
         return _values[i] == null;
      }

      public T Get<T>(int i)
      {
         object v = _values[i];

         if (v == null) return default(T);

         if(!(v is T))
         {
            throw new ArgumentException($"value at {i} is of type '{v.GetType()}' and cannot be casted to '{typeof(T)}'");
         }

         return (T)v;
      }

      public override string ToString()
      {
         return string.Join("; ", _values);
      }
   }
}
