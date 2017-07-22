using System.Collections;
using System.Linq;

namespace Parquet.Data.Stats
{
   /// <summary>
   /// Used to return the number of null values in the column
   /// </summary>
   public class NullStatsHandler : StatsHandler
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
         int totalNulls = values.Values.Cast<object>().Count(value => value == null);
         values.ColumnStats.NullCount = totalNulls;
         return values.ColumnStats;
      }
   }
}