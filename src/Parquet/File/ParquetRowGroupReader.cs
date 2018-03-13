namespace Parquet.File
{
   using System;
   using System.Collections.Generic;
   using System.IO;
   using Parquet.Data;
   using Thrift;


   internal class ParquetRowGroupReader
   {
      readonly RowGroup _rowGroup;
      readonly ThriftFooter _footer;
      readonly Stream _stream;
      readonly ThriftStream _thriftStream;
      readonly ParquetOptions _parquetOptions;
      readonly Dictionary<string, ColumnChunk> _pathToChunk = new Dictionary<string, ColumnChunk>();

      internal ParquetRowGroupReader(
         RowGroup rowGroup,
         ThriftFooter footer,
         Stream stream, ThriftStream thriftStream,
         ParquetOptions parquetOptions)
      {
         _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
         _footer = footer ?? throw new ArgumentNullException(nameof(footer));
         _stream = stream ?? throw new ArgumentNullException(nameof(stream));
         _thriftStream = thriftStream ?? throw new ArgumentNullException(nameof(thriftStream));
         _parquetOptions = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

         //cache chunks
         foreach (var thriftChunk in _rowGroup.Columns)
         {
            string path = thriftChunk.GetPath();
            _pathToChunk[path] = thriftChunk;
         }
      }

      /// <summary>
      /// Gets the number of rows in this row group
      /// </summary>
      public long RowCount => _rowGroup.Num_rows;

      /// <summary>
      /// Reads a column from this row group.
      /// </summary>
      /// <param name="field"></param>
      /// <returns></returns>
      public DataColumn ReadColumn(DataField field)
      {
         if (field == null) throw new ArgumentNullException(nameof(field));

         if (!_pathToChunk.TryGetValue(field.Path, out ColumnChunk columnChunk))
         {
            throw new NotImplementedException();
         }

         var columnReader = new DataColumnReader(field, _stream, columnChunk, _footer, _parquetOptions);

         return columnReader.Read();
      }
   }
}