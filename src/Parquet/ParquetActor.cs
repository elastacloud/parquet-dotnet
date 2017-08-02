using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Base class for reader and writer
   /// </summary>
   public class ParquetActor
   {
      private readonly Stream _fileStream;
      private BinaryReader _binaryReader;
      private BinaryWriter _binaryWriter;
      private ThriftStream _thriftStream;

      //todo: comparer for Schema

      internal ParquetActor(Stream fileStream)
      {
         _fileStream = fileStream ?? throw new ArgumentNullException(nameof(_fileStream));
      }

      internal BinaryReader Reader => _binaryReader ?? (_binaryReader = new BinaryReader(_fileStream));

      internal BinaryWriter Writer => _binaryWriter ?? (_binaryWriter = new BinaryWriter(_fileStream));

      internal ThriftStream ThriftStream => _thriftStream ?? (_thriftStream = new ThriftStream(_fileStream));

      internal void ValidateFile()
      {
         _fileStream.Seek(0, SeekOrigin.Begin);
         char[] head = Reader.ReadChars(4);
         string shead = new string(head);
         _fileStream.Seek(-4, SeekOrigin.End);
         char[] tail = Reader.ReadChars(4);
         string stail = new string(tail);
         if (shead != "PAR1")
            throw new IOException($"not a Parquet file(head is '{shead}')");
         if (stail != "PAR1")
            throw new IOException($"not a Parquet file(head is '{stail}')");
      }
   }
}
