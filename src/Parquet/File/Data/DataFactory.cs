namespace Parquet.File.Data
{
   using System;
   using System.Collections.Generic;
   using System.Linq;
   using Thrift;

   
   static class DataFactory
   {
      static readonly Dictionary<CompressionMethod, KeyValuePair<IDataWriter, IDataReader>> CompressionMethodToWorker = new Dictionary<CompressionMethod, KeyValuePair<IDataWriter, IDataReader>>()
      {
         { CompressionMethod.None, new KeyValuePair<IDataWriter, IDataReader>(new UncompressedDataWriter(), new UncompressedDataReader()) },
         { CompressionMethod.Gzip, new KeyValuePair<IDataWriter, IDataReader>(new GzipDataWriter(), new GzipDataReader()) },
         { CompressionMethod.Snappy, new KeyValuePair<IDataWriter, IDataReader>(new SnappyDataWriter(), new SnappyDataReader()) }
      };

      static readonly Dictionary<CompressionMethod, CompressionCodec> CompressionMethodToCodec = new Dictionary<CompressionMethod, CompressionCodec>
      {
         { CompressionMethod.None, CompressionCodec.UNCOMPRESSED },
         { CompressionMethod.Gzip, CompressionCodec.GZIP },
         { CompressionMethod.Snappy, CompressionCodec.SNAPPY }
      };

      public static CompressionCodec GetThriftCompression(CompressionMethod method)
      {
         if (!CompressionMethodToCodec.TryGetValue(method, out CompressionCodec thriftCodec))
            throw new NotSupportedException($"codec '{method}' is not supported");

         return thriftCodec;
      }

      public static IDataWriter GetWriter(CompressionMethod method)
      {
         return CompressionMethodToWorker[method].Key;
      }

      public static IDataReader GetReader(CompressionMethod method)
      {
         return CompressionMethodToWorker[method].Value;
      }

      public static IDataReader GetReader(CompressionCodec thriftCodec)
      {
         if (!CompressionMethodToCodec.ContainsValue(thriftCodec))
            throw new NotSupportedException($"reader for compression '{thriftCodec}' is not supported.");

         CompressionMethod method = CompressionMethodToCodec.First(kv => kv.Value == thriftCodec).Key;

         return GetReader(method);
      }
   }
}
