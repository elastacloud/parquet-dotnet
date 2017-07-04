using Parquet.Thrift;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data
{
   public class Schema
   {
      private List<SchemaElement> _elements;

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
