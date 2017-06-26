using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Parquet.Test.Reader
{
   public class TestDataTest : ParquetCsvComparison
   {
      [Fact]
      public void Alltypes_plain()
      {
         CompareFiles("alltypes_plain");
      }
   }
}
