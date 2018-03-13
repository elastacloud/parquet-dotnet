namespace Parquet.File.Data
{
   using System.IO;

   
   class UncompressedDataReader :
      IDataReader
   {
      public byte[] Read(Stream source, int count)
      {
         var result = new byte[count];

         source.Read(result, 0, count);

         return result;
      }
   }
}
