using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Parquet.Extensions;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Represents a tabular row
   /// </summary>
   public class Row : IEquatable<Row>
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class which has only one single column.
      /// </summary>
      public Row(object value) : this(new[] { value })
      {

      }

      /// <summary>
      /// Creates a single cell row. Use this method to avoid overloading confusion.
      /// </summary>
      public static Row SingleCell(object value)
      {
         return new Row(value);
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class.
      /// </summary>
      public Row(IEnumerable<object> values)
      {
         Values = values.ToArray();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class.
      /// </summary>
      public Row(params object[] values)
      {
         Values = values;
      }

      /// <summary>
      /// Raw values
      /// </summary>
      public object[] Values { get; }

      /// <summary>
      /// Gets the number of values in this row
      /// </summary>
      public int Length => Values.Length;

      /// <summary>
      /// Gets the row value by index
      /// </summary>
      public object this[int i]
      {
         get
         {
            return Values[i];
         }
      }

      /// <summary>
      /// Gets the value as boolean
      /// </summary>
      /// <param name="i">Value index</param>
      public bool GetBoolean(int i)
      {
         return Get<bool>(i);
      }
      /// <summary>
      /// Gets the value as integer
      /// </summary>
      /// <param name="i">Value index</param>
      public int GetInt(int i)
      {
         return Get<int>(i);
      }

      /// <summary>
      /// Gets the value as float
      /// </summary>
      /// <param name="i">Value index</param>
      public float GetFloat(int i)
      {
         return Get<float>(i);
      }

      /// <summary>
      /// Gets the value as long
      /// </summary>
      /// <param name="i">Value index</param>
      public long GetLong(int i)
      {
         return Get<long>(i);
      }

      /// <summary>
      /// Gets the value as double
      /// </summary>
      /// <param name="i">Value index</param>
      public double GetDouble(int i)
      {
         return Get<double>(i);
      }

      /// <summary>
      /// Gets the value as big integer
      /// </summary>
      /// <param name="i">Value index</param>
      public BigInteger GetBigInt(int i)
      {
         return Get<BigInteger>(i);
      }

      /// <summary>
      /// Gets the value as byte array
      /// </summary>
      /// <param name="i">Value index</param>
      public byte[] GetByteArray(int i)
      {
         return Get<byte[]>(i);
      }

      /// <summary>
      /// Gets the value as string
      /// </summary>
      /// <param name="i">Value index</param>
      public string GetString(int i)
      {
         return Get<string>(i);
      }

      /// <summary>
      /// Gets the value as <see cref="DateTimeOffset"/>
      /// </summary>
      /// <param name="i">Value index</param>
      public DateTimeOffset GetDateTimeOffset(int i)
      {
         return Get<DateTimeOffset>(i);
      }

      /// <summary>
      /// Returns true if value at column <paramref name="i"/> is NULL.
      /// </summary>
      public bool IsNullAt(int i)
      {
         return Values[i] == null;
      }

      /// <summary>
      /// Gets the value trying to cast to <typeparamref name="T"/>
      /// </summary>
      /// <param name="i">Value index</param>
      /// <exception cref="ArgumentException">Cannot cast <typeparamref name="T"/></exception>
      public T Get<T>(int i)
      {
         object v = Values[i];

         if (v == null)
            return default(T);

         if (!(v is T))
         {
            throw new ArgumentException($"value at {i} is of type '{v.GetType()}' and cannot be casted to '{typeof(T)}'");
         }

         return (T)v;
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         var sb = new StringBuilder();
         
         ToString(sb, -1);

         return sb.ToString();
      }

      internal void ToString(StringBuilder sb, int nestLevel)
      {
         sb.OpenBrace(nestLevel);

         bool first = true;
         foreach (object v in Values)
         {
            if (first)
            {
               first = false;
            }
            else
            {
               sb.Append(";");
            }

            FormatValue(v, sb, nestLevel == -1 ? -1 : nestLevel + 1);
         }
         sb.CloseBrace(nestLevel);
      }

      private static void FormatValue(object v, StringBuilder sb, int nestLevel)
      {
         if (v == null)
         {
            sb.Append("null");
         }
         else if (v is Row row)
         {
            row.ToString(sb, nestLevel == -1 ? -1 : nestLevel + 1);
         }
         else if ((!v.GetType().IsSimple()) && v is IEnumerable ien)
         {
            sb.Append("[");
            bool first = true;
            foreach (object cv in ien)
            {
               if (first)
               {
                  first = false;
               }
               else
               {
                  sb.Append(";");
               }

               FormatValue(cv, sb, nestLevel == -1 ? -1 : nestLevel + 1);
            }
            sb.Append("]");
         }
         else
         {
            sb.Append(v.ToString());
         }

      }

      /// <summary>
      /// 
      /// </summary>
      public bool Equals(Row other)
      {
         return Equals(other, false);
      }

      /// <summary>
      /// 
      /// </summary>
      public bool Equals(Row other, bool throwException)
      {
         if (Values.Length != other.Values.Length)
         {
            if(throwException)
            {
               throw new ArgumentException($"values count is different ({Values.Length} != {other.Values.Length})");
            }

            return false;
         }

         for(int i = 0; i < Values.Length; i++)
         {
            object v = Values[i];
            object ov = other.Values[i];

            if(v == null || ov == null)
            {
               bool equal = v == null && ov == null;

               if(!equal && throwException)
               {
                  throw new ArgumentException($"only one of the values is null at position {i}");
               }

               return equal;
            }

            if (!v.Equals(ov))
            {
               if(throwException)
               {
                  throw new ArgumentException($"values are not equal at position {i} ({v} != {ov})");
               }

               return false;
            }
         }

         return true;
      }
   }
}