namespace Parquet.File.Data
{
   using System.IO;
   using Snappy.Sharp;


   class SnappyDataReader :
      IDataReader
   {
      readonly SnappyDecompressor _snappyDecompressor = new SnappyDecompressor();

      public byte[] Read(Stream source, int count)
      {
         var buffer = new byte[count];
         source.Read(buffer, 0, count);
         var uncompressedBytes = _snappyDecompressor.Decompress(buffer, 0, count);

         return uncompressedBytes;
      }
   }
}
