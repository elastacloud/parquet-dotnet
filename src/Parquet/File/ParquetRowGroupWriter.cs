namespace Parquet.File
{
   using System;
   using System.Collections.Generic;
   using System.IO;
   using System.Linq;
   using Parquet.Data;
   using Thrift;
   using Values;

   
   internal class ParquetRowGroupWriter :
      IDisposable
   {
      readonly Schema _schema;
      readonly Stream _stream;
      readonly ThriftStream _thriftStream;
      readonly ThriftFooter _footer;
      readonly CompressionMethod _compressionMethod;
      readonly ParquetOptions _formatOptions;
      readonly int _rowCount;
      readonly RowGroup _thriftRowGroup;
      readonly long _rgStartPos;
      readonly List<SchemaElement> _thschema;
      int _colIdx;

      struct PageTag
      {
         public int HeaderSize;
         public PageHeader HeaderMeta;
      }

      internal ParquetRowGroupWriter(Schema schema,
         Stream stream,
         ThriftStream thriftStream,
         ThriftFooter footer, 
         CompressionMethod compressionMethod,
         ParquetOptions formatOptions,
         int rowCount)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _stream = stream ?? throw new ArgumentNullException(nameof(stream));
         _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _compressionMethod = compressionMethod;
         _formatOptions = formatOptions;
         _rowCount = rowCount;

         _thriftRowGroup = _footer.AddRowGroup();
         _thriftRowGroup.Num_rows = _rowCount;
         _rgStartPos = _stream.Position;
         _thriftRowGroup.Columns = new List<ColumnChunk>();
         _thschema = _footer.GetWriteableSchema().ToList();
      }

      public void Write(DataColumn column)
      {
         if (column == null) throw new ArgumentNullException(nameof(column));

         SchemaElement tse = _thschema[_colIdx++];
         IDataTypeHandler dataTypeHandler = DataTypeFactory.Match(tse, _formatOptions);
         //todo: check if the column is in the right order

         List<string> path = _footer.GetPath(tse);

         ColumnChunk chunk = WriteColumnChunk(tse, path, column, dataTypeHandler);
         _thriftRowGroup.Columns.Add(chunk);
      }

      ColumnChunk WriteColumnChunk(SchemaElement tse, List<string> path, DataColumn column, IDataTypeHandler dataTypeHandler)
      {
         ColumnChunk chunk = _footer.CreateColumnChunk(_compressionMethod, _stream, tse.Type, path, 0);
         var ph = _footer.CreateDataPage(_rowCount);
         _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);

         List<PageTag> pages = WriteColumn(column, tse, dataTypeHandler, maxRepetitionLevel, maxDefinitionLevel);

         chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;

         //the following counters must include both data size and header size
         chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
         chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

         return chunk;
      }

      List<PageTag> WriteColumn(DataColumn column, 
         SchemaElement tse,
         IDataTypeHandler dataTypeHandler,
         int maxRepetitionLevel,
         int maxDefinitionLevel)
      {
         var pages = new List<PageTag>();

         /*
          * Page header must preceeed actual data (compressed or not) however it contains both
          * the uncompressed and compressed data size which we don't know! This somehow limits
          * the write efficiency.
          */

         using (var ms = new MemoryStream())
         {
            PageHeader dataPageHeader = _footer.CreateDataPage(column.TotalCount);

            //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
            using (PositionTrackingStream pps = DataStreamFactory.CreateWriter(ms, _compressionMethod))
            {
               using (var writer = new BinaryWriter(pps))
               {
                  if (column.HasRepetitions)
                     throw new NotImplementedException();

                  if (column.HasDefinitions)
                  {
                     WriteLevels(writer, column.DefinitionLevels, maxDefinitionLevel);
                  }

                  dataTypeHandler.Write(tse, writer, column.DefinedData);
               }

               dataPageHeader.Uncompressed_page_size = (int)pps.Position;
            }
            
            dataPageHeader.Compressed_page_size = (int)ms.Position;

            //write the hader in
            int headerSize = _thriftStream.Write(dataPageHeader);
            ms.Position = 0;
            ms.CopyTo(_stream);

            var dataTag = new PageTag
            {
               HeaderMeta = dataPageHeader,
               HeaderSize = headerSize
            };

            pages.Add(dataTag);
         }

         return pages;
      }

      void WriteLevels(BinaryWriter writer, List<int> levels, int maxLevel)
      {
         int bitWidth = maxLevel.GetBitWidth();
         RunLengthBitPackingHybridValuesWriter.WriteForwardOnly(writer, bitWidth, levels);
      }

      public void Dispose()
      {
         //todo: check if all columns are present

         //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
         //luckily ColumnChunk already contains sizes of page+header in it's meta
         _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);
      }
   }
}