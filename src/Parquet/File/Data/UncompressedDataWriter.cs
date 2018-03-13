namespace Parquet.File.Data
{
   using System.IO;

   
   class UncompressedDataWriter :
      IDataWriter
   {
      public void Write(byte[] buffer, Stream destination)
      {
         destination.Write(buffer, 0, buffer.Length);
      }
   }
}
