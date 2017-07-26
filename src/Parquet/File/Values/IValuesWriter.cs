using Parquet.Data;
using System.Collections;
using System.IO;

namespace Parquet.File.Values
{
   interface IValuesWriter
   {
      /// <summary>
      /// Writes the specified writer.
      /// </summary>
      /// <param name="writer">The writer.</param>
      /// <param name="schema">The schema.</param>
      /// <param name="data">The data.</param>
      /// <returns>False if this encoding cannot write specified data</returns>
      bool Write(BinaryWriter writer, SchemaElement schema, IList data);
   }
}
