using System;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using Xunit;

namespace Parquet.Test
{
   using File = System.IO.File;

   /// <summary>
   /// Tests a set of predefined test files that they read back correct
   /// </summary>
   public class ParquetReaderOnTestFilesTest
   {
      private byte[] vals = new byte[18]
      {
         0x00,
         0x00,
         0x27,
         0x79,
         0x7f,
         0x26,
         0xd6,
         0x71,
         0xc8,
         0x00,
         0x00,
         0x4e,
         0xf2,
         0xfe,
         0x4d,
         0xac,
         0xe3,
         0x8f
      };

      [Fact]
      public void FixedLenByteArray_dictionary()
      {
         using (Stream s = File.OpenRead(GetDataFilePath("fixedlenbytearray.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();
            }
         }
      }

      [Fact]
      public void Datetypes_all()
      {
         using (Stream s = File.OpenRead(GetDataFilePath("dates.parquet")))
         {
            using (var r = new ParquetReader(s))
            {
               ParquetDataSet ds = r.Read();
            }
         }
      }

      [Fact]
      public void test_compress()
      {
         var output = Compress(vals);
         var decomp = Decompress(output);
         Assert.Equal(vals, decomp);
      }

      public static byte[] Compress(byte[] raw)
      {
         using (MemoryStream memory = new MemoryStream())
         {
            using (GZipStream gzip = new GZipStream(memory,
                CompressionMode.Compress, true))
            {
               gzip.Write(raw, 0, raw.Length);
            }
            return memory.ToArray();
         }
      }

      static byte[] Decompress(byte[] gzip)
      {
         // Create a GZIP stream with decompression mode.
         // ... Then create a buffer and write into while reading from the GZIP stream.
         using (GZipStream stream = new GZipStream(new MemoryStream(gzip),
            CompressionMode.Decompress))
         {
            const int size = 4096;
            byte[] buffer = new byte[size];
            using (MemoryStream memory = new MemoryStream())
            {
               int count = 0;
               do
               {
                  count = stream.Read(buffer, 0, size);
                  if (count > 0)
                  {
                     memory.Write(buffer, 0, count);
                  }
               }
               while (count > 0);
               return memory.ToArray();
            }
         }
      }



      private string GetDataFilePath(string name)
      {
         string thisPath = Assembly.Load(new AssemblyName("Parquet.Test")).Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}