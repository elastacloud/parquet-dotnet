﻿namespace Parquet.File.Data
{
   using System.IO;

   
   //note that this may be obsolete in next major version
   interface IDataWriter
   {
      void Write(byte[] buffer, Stream destination);
   }
}
