namespace Parquet.File.Data
{
   using System.IO;
   using Snappy.Sharp;

   
   class SnappyDataWriter :
      IDataWriter
   {
      readonly SnappyCompressor _snappyCompressor = new SnappyCompressor();
      
      public void Write(byte[] buffer, Stream destination)
      {
         int uncompressedLength = buffer.Length;
         int compressedSize = _snappyCompressor.MaxCompressedLength(uncompressedLength);
         byte[] compressed = new byte[compressedSize];
         int length = _snappyCompressor.Compress(buffer, 0, uncompressedLength, compressed);

         destination.Write(compressed, 0, length);
      }
   }
}
