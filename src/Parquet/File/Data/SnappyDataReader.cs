using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Snappy.Sharp;

namespace Parquet.File.Data
{
    class SnappyDataReader: IDataReader
    {
       public byte[] Read(Stream source, int count)
       {
          var buffer = new byte[count];
          source.Read(buffer, 0, count);
          var snappy = new SnappyDecompressor();
          var uncompressedBytes = snappy.Decompress(buffer, 0, count);
          return uncompressedBytes;
       }
    }
}
