using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data.Rows;
using Parquet.Extensions;
using Xunit;

namespace Parquet.Test.Extensions
{
   public class StringBuilderExtensionsTest : TestBase
   {
      [Fact]
      public void NonNull_objects_create_string()
      {
         var sb = new StringBuilder();
         StringFormat sf = StringFormat.Json;

         sb.StartArray(sf, 1);

         sb.Append(sf, "Hello");
         sb.DivideObjects(sf, 1);
         sb.Append(sf, "213");

         sb.EndArray(sf, 1);

         Assert.Equal("[\"Hello\",\"213\"]", sb.ToString());
      }

      [Fact]
      public void Null_objects_create_json_null_value()
      {
         var sb = new StringBuilder();
         StringFormat sf = StringFormat.Json;

         sb.StartArray(sf, 1);

         sb.Append(sf, "Null Test");
         sb.DivideObjects(sf, 1);
         sb.Append(sf, null);

         sb.EndArray(sf, 1);

         Assert.Equal("[\"Null Test\",null]", sb.ToString());
      }
   }
}
