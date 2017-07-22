using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data.Stats;

namespace Parquet.Data
{
   class DataSetStats
   {
      private readonly DataSet _ds;
      private readonly Dictionary<StatsHandler, Type[]> _handlers = new Dictionary<StatsHandler, Type[]>();

      public DataSetStats(DataSet ds)
      {
         _ds = ds;
         _handlers.Add(new MaxStatsHandler(), NumericTypes);
         _handlers.Add(new NullStatsHandler(), AllTypes);
         _handlers.Add(new MinStatsHandler(), NumericTypes);
         _handlers.Add(new MeanStatsHandler(), NumericTypes);
         _handlers.Add(new StdDevHandler(), NumericTypes);
      }

      private Type[] NumericTypes => new Type[] {typeof(double), typeof(int), typeof(float), typeof(long)};
      private Type[] AllTypes => new Type[] { typeof(double), typeof(int), typeof(float), typeof(long), typeof(string), typeof(DateTimeOffset) };

      public ColumnStats GetColumnStats(int index)
      {
         var stats = new ColumnStats(_ds.Schema.ColumnNames[index]);
         var columns = _ds.GetColumn(index);
         
         foreach (var handler in _handlers)
         {
            // This should be responsible for it's own types
            handler.Key.GetColumnStats(new ColumnStatsDetails(columns, stats, handler.Value, _ds.Schema.Elements[index].ElementType));
         }

         return stats;
      }

      public ColumnStats GetColumnStats(SchemaElement schema)
      {
         int index = _ds.Schema.GetElementIndex(schema);
         return GetColumnStats(index);
      }

   }
}
