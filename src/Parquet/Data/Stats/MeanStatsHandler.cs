using System;
using System.Collections;
using System.Linq;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Used to return the min value of the column
   /// </summary>
   public class MeanStatsHandler : StatsHandler
   {
      /// <summary>
      /// Gets the count of null values given the list of column values
      /// </summary>
      /// <param name="values">A list of values</param>
      /// <returns>A count of null values</returns>
      public override ColumnStats GetColumnStats(ColumnStatsDetails values)
      {
         if (!CanCalculateWithType(values))
            return values.ColumnStats;
         double count = values.Values.Count;
         double sum = values.Values.Cast<object>().Sum(value => Convert.ToDouble(value));
         values.ColumnStats.Mean = sum / count;
         return values.ColumnStats;
      }
   }
}