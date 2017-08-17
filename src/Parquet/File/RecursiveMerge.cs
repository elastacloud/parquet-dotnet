using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Merges results into flat <see cref="DataSet"/>
   /// </summary>
   class RecursiveMerge
   {
      private readonly Schema _schema;

      public RecursiveMerge(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
      }

      public DataSet Merge(Dictionary<string, IList> pathToValues)
      {
         int count = pathToValues.Min(e => e.Value.Count);

         var ds = new DataSet(_schema);

         return ds;
      }
   }
}
