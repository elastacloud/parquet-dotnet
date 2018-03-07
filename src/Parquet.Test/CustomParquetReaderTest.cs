using Parquet.Data;
using Parquet.File;
using Parquet.Thrift;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Parquet.Test
{
   public class CustomParquetReaderTest : TestBase
   {  
      [Fact]
      public void Read_from_parquet_file_stream()
      {
         Stream parquetStream = System.IO.File.OpenRead("data/alltypes.plain.parquet");
         ParquetOptions parquetOptions = new ParquetOptions()
         {
            TreatByteArrayAsString = true
         };
         ParquetActor parquetActor = new ParquetActor(parquetStream);
         FileMetaData meta = parquetActor.ReadMetadata();
         ThriftFooter footer = new ThriftFooter(meta);
         Schema parquetSchema = footer.CreateModelSchema(parquetOptions);
         List<RowGroup> rowGroups = meta.Row_groups;

         Assert.Equal(11, parquetSchema.Length);
         Assert.Equal(8, meta.Num_rows);
         Assert.Single(rowGroups);
         Assert.Equal(8, rowGroups[0].Num_rows);

         ColumnChunk cc = rowGroups[0].Columns[0];
         ColumnarReader columnarReader = new ColumnarReader(parquetStream, cc, footer, parquetOptions);
         IList values = columnarReader.Read(0, 1);
         Assert.Equal(4, values[0]);
      }
   }
}
