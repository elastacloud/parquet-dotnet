using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Snappy.Sharp;

namespace Parquet.File.Data
{
    class SnappyDataWriter : IDataWriter
    {
       public void Write(byte[] buffer, Stream destination)
       {
          var uncompressedLength = buffer.Length;
          var snappy = new SnappyCompressor();
          var compressed = new byte[snappy.MaxCompressedLength(uncompressedLength)];
          snappy.Compress(buffer, 0, uncompressedLength, compressed);
          destination.Write(compressed, 0, compressed.Length);
       }
    }
}
