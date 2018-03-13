﻿namespace Parquet.File
{
   using System.Collections;
   using System.Collections.Generic;
   using System.IO;
   using System.Linq;
   using Parquet.Data;
   using Data;
   using Thrift;
   using Values;

   
   class ColumnarWriter
   {
      readonly Stream _output;
      readonly ThriftStream _thriftStream;
      readonly ThriftFooter _footer;
      readonly SchemaElement _tse;
      readonly CompressionMethod _compressionMethod;
      readonly ParquetOptions _formatOptions;
      readonly WriterOptions _writerOptions;
      readonly IDataTypeHandler _dataTypeHandler;
      readonly ColumnChunk _chunk;
      readonly PageHeader _ph;
      readonly int _maxRepetitionLevel;
      readonly int _maxDefinitionLevel;

      struct PageTag
      {
         public int HeaderSize;
         public PageHeader HeaderMeta;
      }

      public ColumnarWriter(Stream output, ThriftStream thriftStream,
         ThriftFooter footer,
         SchemaElement tse, List<string> path,
         CompressionMethod compressionMethod,
         ParquetOptions formatOptions,
         WriterOptions writerOptions)
      {
         _output = output;
         _thriftStream = thriftStream;
         _footer = footer;
         _tse = tse;
         _compressionMethod = compressionMethod;
         _formatOptions = formatOptions;
         _writerOptions = writerOptions;
         _dataTypeHandler = DataTypeFactory.Match(tse, _formatOptions);

         _chunk = _footer.CreateColumnChunk(_compressionMethod, _output, _tse.Type, path, 0);
         _ph = _footer.CreateDataPage(0);
         _footer.GetLevels(_chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);
         _maxRepetitionLevel = maxRepetitionLevel;
         _maxDefinitionLevel = maxDefinitionLevel;
      }

      public ColumnChunk Write(int offset, int count, IList values)
      {
         //note that values can be null, meaning there are no values at all

         _ph.Data_page_header.Num_values = values == null ? 0 : values.Count;
         List<PageTag> pages = WriteValues(values);
         _chunk.Meta_data.Num_values = _ph.Data_page_header.Num_values;

         //the following counters must include both data size and header size
         _chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         _chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return _chunk;
      }

      List<PageTag> WriteValues(IList values)
      {
         var result = new List<PageTag>();
         byte[] dataPageBytes;
         List<int> repetitions = null;
         List<int> definitions = null;
         List<bool> hasValueFlags = null;

         if (values != null)
         {
            //flatten values and create repetitions list if the field is repeatable
            if (_maxRepetitionLevel > 0)
            {
               repetitions = new List<int>();
               IList flatValues = _dataTypeHandler.CreateEmptyList(_tse.IsNullable(), false, 0);
               hasValueFlags = new List<bool>();
               RepetitionPack.HierarchyToFlat(_maxRepetitionLevel, values, flatValues, repetitions, hasValueFlags);
               values = flatValues;
               _ph.Data_page_header.Num_values = values.Count; //update with new count
            }

            if (_maxDefinitionLevel > 0)
            {
               definitions = DefinitionPack.RemoveNulls(values, _maxDefinitionLevel, hasValueFlags);
            }

            TryAddStats(values);
         }

         using (var ms = new MemoryStream())
         {
            using (var writer = new BinaryWriter(ms))
            {
               if (values != null)
               {
                  //write repetitions
                  if (repetitions != null)
                  {
                     WriteLevels(writer, repetitions, _maxRepetitionLevel);
                  }

                  //write definitions
                  if (definitions != null)
                  {
                     WriteLevels(writer, definitions, _maxDefinitionLevel);
                  }

                  //write data
                  _dataTypeHandler.Write(_tse, writer, values);
               }

               dataPageBytes = ms.ToArray();
            }
         }

         dataPageBytes = Compress(dataPageBytes);
         int dataHeaderSize = Write(dataPageBytes);
         result.Add(new PageTag { HeaderSize = dataHeaderSize, HeaderMeta = _ph });

         return result;
      }

      void TryAddStats(IList flatValues)
      {
         var counter = new StatCounter(flatValues);
      }

      void WriteLevels(BinaryWriter writer, List<int> levels, int maxLevel)
      {
         int bitWidth = maxLevel.GetBitWidth();
         RunLengthBitPackingHybridValuesWriter.Write(writer, bitWidth, levels);
      }

      int Write(byte[] data)
      {
         int headerSize = _thriftStream.Write(_ph);
         _output.Write(data, 0, data.Length);
         return headerSize;
      }

      byte[] Compress(byte[] data)
      {
         //note that page size numbers do not include header size by spec

         _ph.Uncompressed_page_size = data.Length;
         byte[] result;

         if (_compressionMethod != CompressionMethod.None)
         {
            IDataWriter writer = DataFactory.GetWriter(_compressionMethod);
            using (var ms = new MemoryStream())
            {
               writer.Write(data, ms);
               result = ms.ToArray();
            }
            _ph.Compressed_page_size = result.Length;
         }
         else
         {
            _ph.Compressed_page_size = _ph.Uncompressed_page_size;
            result = data;
         }

         return result;
      }
   }
}
