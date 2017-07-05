using Parquet.Data;
using System.IO;
using Xunit;
using F = System.IO.File;

namespace Parquet.Test
{
   public class ParquetWriterTest
   {
      [Fact]
      public void Write_simple_int32_and_int_reads_back()
      {
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(ms))
         {
            var ds = new DataSet(
               new SchemaElement<int>("id", false),
               new SchemaElement<bool>("bool_col", false),
               new SchemaElement<string>("string_col", false)
            );

            //8 values for each column

            ds.Add(4, true, "0");
            ds.Add(5, false, "1");
            ds.Add(6, true, "0");
            ds.Add(7, false, "1");
            ds.Add(2, true, "0");
            ds.Add(3, false, "1");
            ds.Add(0, true, "0");
            ds.Add(1, false, "0");

            writer.Write(ds);
         }

         ms.Position = 0;
         using (var reader = new ParquetReader(ms))
         {
            DataSet ds = reader.Read();
         }

#if DEBUG
         const string path = "c:\\tmp\\first.parquet";
         F.WriteAllBytes(path, ms.ToArray());
#endif

      }
   }
}
