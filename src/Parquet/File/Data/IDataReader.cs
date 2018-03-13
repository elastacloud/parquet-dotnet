namespace Parquet.File.Data
{
   using System.IO;

   
   interface IDataReader
   {
      byte[] Read(Stream source, int count);
   }
}