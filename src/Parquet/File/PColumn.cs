﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.File.Values;
using Parquet.Data;
using Parquet.File.Data;

namespace Parquet.File
{
   class PColumn
   {
      private readonly Thrift.ColumnChunk _thriftChunk;
      private readonly Stream _inputStream;
      private readonly ThriftStream _thrift;
      private readonly Schema _schema;
      private readonly SchemaElement _schemaElement;
      private readonly ParquetOptions _options;

      private readonly IValuesReader _plainReader;
      private static readonly IValuesReader _rleReader = new RunLengthBitPackingHybridValuesReader();
      private static readonly IValuesReader _dictionaryReader = new PlainDictionaryValuesReader();

      public PColumn(Thrift.ColumnChunk thriftChunk, Schema schema, Stream inputStream, ThriftStream thriftStream, ParquetOptions options)
      {
         _thriftChunk = thriftChunk;
         _thrift = thriftStream;
         _schema = schema;
         _inputStream = inputStream;
         _schemaElement = _schema[_thriftChunk];
         _options = options;

         _plainReader = new PlainValuesReader(options);
      }

      public IList Read(string columnName, long offset, long count)
      {
         IList values = TypeFactory.Create(_schemaElement, _options);

         //get the minimum offset, we'll just read pages in sequence
         long fileOffset = new[] { _thriftChunk.Meta_data.Dictionary_page_offset, _thriftChunk.Meta_data.Data_page_offset }.Where(e => e != 0).Min();
         long maxValues = _thriftChunk.Meta_data.Num_values;

         _inputStream.Seek(fileOffset, SeekOrigin.Begin);

         Thrift.PageHeader ph = _thrift.Read<Thrift.PageHeader>();

         IList dictionaryPage = null;
         List<int> indexes = null;
         List<int> definitions = null;
         List<int> repetitions = null;

         //there can be only one dictionary page in column
         if (ph.Type == Thrift.PageType.DICTIONARY_PAGE)
         {
            dictionaryPage = ReadDictionaryPage(ph);
            ph = _thrift.Read<Thrift.PageHeader>(); //get next page after dictionary
         }

         int dataPageCount = 0;
         while (true)
         {
            int valuesSoFar = Math.Max(indexes == null ? 0 : indexes.Count, values.Count);
            var page = ReadDataPage(ph, values, maxValues - valuesSoFar);

            indexes = AssignOrAdd(indexes, page.indexes);
            definitions = AssignOrAdd(definitions, page.definitions);
            repetitions = AssignOrAdd(repetitions, page.repetitions);

            dataPageCount++;

            int totalCount = Math.Max(

               values.Count +
               (indexes == null
                  ? 0
                  : indexes.Count),

               definitions == null ? 0 : definitions.Count);

            if (totalCount >= maxValues) break; //limit reached

            ph = ReadDataPageHeader(dataPageCount); //get next page

            if (ph.Type != Thrift.PageType.DATA_PAGE) break;
         }

         IList mergedValues = new ValueMerger(_schemaElement, _options, values)
            .Apply(dictionaryPage, definitions, repetitions, indexes, (int)maxValues);

         ValueMerger.Trim(mergedValues, (int)offset, (int)count);

         return mergedValues;
      }

      private List<int> AssignOrAdd(List<int> container, List<int> source)
      {
         if (source != null)
         {

            if (container == null)
            {
               container = source;
            }
            else
            {
               container.AddRange(source);
            }
         }

         return container;
      }

      private Thrift.PageHeader ReadDataPageHeader(int pageNo)
      {
         Thrift.PageHeader ph;

         try
         {
            ph = _thrift.Read<Thrift.PageHeader>();
         }
         catch (Exception ex)
         {
            throw new IOException($"failed to read data page header after page #{pageNo}", ex);
         }

         if (ph.Type != Thrift.PageType.DATA_PAGE)
         {
            throw new IOException($"expected data page but read {ph.Type}");
         }

         return ph;
      }

      private IList ReadDictionaryPage(Thrift.PageHeader ph)
      {
         //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain enncoding.

         byte[] data = ReadRawBytes(ph, _inputStream);

         using (var dataStream = new MemoryStream(data))
         {
            using (var dataReader = new BinaryReader(dataStream))
            {
               IList result = TypeFactory.Create(_schemaElement, _options);
               _plainReader.Read(dataReader, _schemaElement, result, int.MaxValue);
               return result;
            }
         }
      }

      private (List<int> definitions, List<int> repetitions, List<int> indexes) ReadDataPage(Thrift.PageHeader ph, IList destination, long maxValues)
      {
         byte[] data = ReadRawBytes(ph, _inputStream);
         int max = ph.Data_page_header.Num_values;

         using (var dataStream = new MemoryStream(data))
         {
            using (var reader = new BinaryReader(dataStream))
            {
               List<int> repetitions = _schemaElement.HasRepetitionLevelsPage
                  ? ReadRepetitionLevels(reader, (int)maxValues)
                  : null;

               List<int> definitions = _schemaElement.HasDefinitionLevelsPage
                  ? ReadDefinitionLevels(reader, (int)maxValues)
                  : null;

               // these are pointers back to the Values table - lookup on values 
               List<int> indexes = ReadColumnValues(reader, ph.Data_page_header.Encoding, destination, max);

               //trim output if it exceeds max number of values
               int numValues = ph.Data_page_header.Num_values;
               if (repetitions != null) ValueMerger.TrimTail(repetitions, numValues);
               if (definitions != null) ValueMerger.TrimTail(definitions, numValues);
               if (indexes != null) ValueMerger.TrimTail(indexes, numValues);

               return (definitions, repetitions, indexes);
            }
         }
      }

      private List<int> ReadRepetitionLevels(BinaryReader reader, int valueCount)
      {
         int maxLevel = _schemaElement.MaxRepetitionLevel;
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxLevel);
         var result = new List<int>();
         //todo: there might be more data on larger files, therefore line below need to be called in a loop until valueCount is satisfied
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, 0, result, valueCount);

         ValueMerger.TrimTail(result, valueCount);  //trim result so null count procudes correct value

         return result;
      }

      private List<int> ReadDefinitionLevels(BinaryReader reader, int valueCount)
      {
         int maxLevel = _schemaElement.MaxDefinitionLevel;
         int bitWidth = PEncoding.GetWidthFromMaxInt(maxLevel);
         var result = new List<int>();
         //todo: there might be more data on larger files, therefore line below need to be called in a loop until valueCount is satisfied
         RunLengthBitPackingHybridValuesReader.ReadRleBitpackedHybrid(reader, bitWidth, 0, result, valueCount);

         ValueMerger.TrimTail(result, valueCount);  //trim result so null count procudes correct value

         return result;
      }

      private List<int> ReadColumnValues(BinaryReader reader, Thrift.Encoding encoding, IList destination, long maxValues)
      {
         //dictionary encoding uses RLE to encode data

         switch(encoding)
         {
            case Thrift.Encoding.PLAIN:
               _plainReader.Read(reader, _schemaElement, destination, maxValues);
               return null;

            case Thrift.Encoding.RLE:
               var rleIndexes = new List<int>();
               _rleReader.Read(reader, _schemaElement, rleIndexes, maxValues);
               return rleIndexes;

            case Thrift.Encoding.PLAIN_DICTIONARY:
               var dicIndexes = new List<int>();
               _dictionaryReader.Read(reader, _schemaElement, dicIndexes, maxValues);
               return dicIndexes;

            default:
               throw new ParquetException($"encoding {encoding} is not supported.");  //todo: replace with own exception type
         }
      }

      private byte[] ReadRawBytes(Thrift.PageHeader ph, Stream inputStream)
      {
         Thrift.CompressionCodec thriftCodec = _thriftChunk.Meta_data.Codec;
         IDataReader reader = DataFactory.GetReader(thriftCodec);

         return reader.Read(inputStream, ph.Compressed_page_size);
      }
   }
}
