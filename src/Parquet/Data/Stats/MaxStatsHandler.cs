using System;
using System.Collections;
using System.Linq;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Used to return the min value of the column
   /// </summary>
   public class MaxStatsHandler : StatsHandler
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
         double max = Convert.ToDouble(values.Values.Cast<object>().Max());
         values.ColumnStats.Max = max;
         return values.ColumnStats;
      }

   }
}