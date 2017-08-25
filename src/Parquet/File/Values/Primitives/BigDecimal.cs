using System.Linq;
using System.Numerics;

namespace Parquet.File.Values.Primitives
{
   /// <summary>
   /// A class that encapsulates BigDecimal like the java class
   /// </summary>
   struct BigDecimal
   {
      /// <summary>
      /// Contains a Decimal value that is the big integer
      /// </summary>
      public decimal OriginalValue { get; set; }

      public decimal Value { get; set; }

      /// <summary>
      /// The scale of the decimal value
      /// </summary>
      public int Scale { get; set; }

      /// <summary>
      /// The precision of the decimal value
      /// </summary>
      public int Precision { get; set; }

      public BigDecimal(byte[] data, Thrift.SchemaElement schema)
      {
         data = data.Reverse().ToArray();
         OriginalValue = (decimal)(new BigInteger(data));
         Scale = schema.Scale;
         Precision = schema.Precision;

         decimal itv = OriginalValue;
         int itsc = Scale;
         while (itsc > 0)
         {
            itv /= 10;
            itsc -= 1;
         }

         Value = itv;
      }

      /// <summary>
      /// Converts a BigDecimal to a decimal
      /// </summary>
      /// <param name="bd">The BigDecimal value</param>
      public static implicit operator decimal(BigDecimal bd)
      {
         return bd.Value;
      }
   }
}