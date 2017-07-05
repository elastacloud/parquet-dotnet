using Parquet.Thrift;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset schema
   /// </summary>
   public class Schema
   {
      private List<SchemaElement> _elements;

      /// <summary>
      /// Initializes a new instance of the <see cref="Schema"/> class from schema elements.
      /// </summary>
      /// <param name="elements">The elements.</param>
      public Schema(IEnumerable<SchemaElement> elements)
      {
         _elements = elements.ToList();
      }

      internal Schema(FileMetaData fm)
      {
         _elements = fm.Schema.Skip(1).Select(se => new SchemaElement(se)).ToList();
      }
   }
}
