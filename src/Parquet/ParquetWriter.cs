/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using System;
using System.IO;
using System.Text;
using Parquet.Thrift;
using Parquet.File;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.File.Values;
using TEncoding = Parquet.Thrift.Encoding;

namespace Parquet
{
   /// <summary>
   /// Implements Apache Parquet format writer
   /// </summary>
   public class ParquetWriter : IDisposable
   {
      private readonly Stream _output;
      private readonly BinaryWriter _writer;
      private readonly ThriftStream _thrift;
      private static readonly byte[] Magic = System.Text.Encoding.ASCII.GetBytes("PAR1");
      private readonly FileMetaData _meta = new FileMetaData();
      private readonly IValuesWriter _plainWriter = new PlainValuesWriter();

      public ParquetWriter(Stream output)
      {
         _output = output ?? throw new ArgumentNullException(nameof(output));
         if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
         _thrift = new ThriftStream(output);
         _writer = new BinaryWriter(_output);

         //file starts with magic
         WriteMagic();

         _meta.Created_by = "parquet-dotnet";
         _meta.Version = 1;
         _meta.Row_groups = new List<RowGroup>();
      }

      public void Write(ParquetDataSet dataSet)
      {
         long totalCount = dataSet.Count;

         _meta.Schema = new List<SchemaElement> { new SchemaElement("schema") { Num_children = dataSet.Columns.Count } };
         _meta.Schema.AddRange(dataSet.Columns.Select(c => c.Schema));
         _meta.Num_rows = totalCount;

         var rg = new RowGroup();
         long rgStartPos = _output.Position;
         _meta.Row_groups.Add(rg);
         rg.Columns = dataSet.Columns.Select(c => Write(c)).ToList();
         rg.Total_byte_size = _output.Position - rgStartPos;
      }

      private ColumnChunk Write(ParquetColumn column)
      {
         var chunk = new ColumnChunk();
         long startPos = _output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new ColumnMetaData();
         chunk.Meta_data.Num_values = column.Values.Count;
         chunk.Meta_data.Type = column.Type;
         chunk.Meta_data.Codec = CompressionCodec.UNCOMPRESSED;   //todo: compression should be passed as parameter
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<TEncoding>
         {
            TEncoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = new List<string> { column.Name };

         var ph = new PageHeader(PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new DataPageHeader
         {
            Encoding = TEncoding.PLAIN,
            Num_values = column.Values.Count
         };

         _thrift.Write(ph);
         _writer.Write((int)0);
         _plainWriter.Write(_writer, column.Schema, column.Values);

         ph.Compressed_page_size = ph.Uncompressed_page_size = (int)(_output.Position - startPos);

         chunk.Meta_data.Total_compressed_size = chunk.Meta_data.Total_uncompressed_size = _output.Position - startPos;

         return chunk;
      }

      private void WriteMagic()
      {
         _output.Write(Magic, 0, Magic.Length);
      }

      public void Dispose()
      {
         //finalize file
         long size = _thrift.Write(_meta);

         //metadata size
         _writer.Write((int)size);  //4 bytes

         //end magic
         WriteMagic();              //4 bytes

         _writer.Flush();
         _output.Flush();
      }
   }
}
