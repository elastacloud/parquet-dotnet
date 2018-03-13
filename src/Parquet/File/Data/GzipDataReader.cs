namespace Parquet.File.Data
{
   using System;
   using System.IO;
   using System.IO.Compression;

   
   class GzipDataReader :
      IDataReader
   {
      public byte[] Read(Stream source, int count)
      {
         byte[] srcBytes = new byte[count];
         source.Read(srcBytes, 0, srcBytes.Length);
         return Decompress(srcBytes);
      }

      static byte[] Decompress(byte[] source)
      {
         using (var sourceStream = new MemoryStream(source))
         {
            using (var destinationStream = new MemoryStream())
            {
               Decompress(sourceStream, destinationStream);
               return destinationStream.ToArray();
            }
         }
      }

      static void Decompress(Stream source, Stream destination)
      {
         if (source == null) throw new ArgumentNullException(nameof(source));
         if (destination == null) throw new ArgumentNullException(nameof(destination));

         using (var decompressor = new GZipStream(source, CompressionMode.Decompress, true))
         {
            decompressor.CopyTo(destination);
            destination.Flush();
         }
      }
   }
}
