﻿using NetBox.Model;
using Parquet.Data;
using Parquet.File.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using TSchemaElement = Parquet.Thrift.SchemaElement;

namespace Parquet.File
{
   class MetaBuilder
   {
      private Thrift.FileMetaData _meta;
      private static readonly string CreatedBy;

      static MetaBuilder()
      {
         //get file version
         Assembly asm = typeof(MetaBuilder).GetTypeInfo().Assembly;
         var fva = asm.CustomAttributes.First(a => a.AttributeType == typeof(AssemblyFileVersionAttribute));
         CustomAttributeTypedArgument varg = fva.ConstructorArguments[0];
         string fileVersion = varg.Value.ToString();

         CreatedBy = $"parquet-dotnet version {fileVersion} (build {fileVersion.GetHash(HashType.Sha1)})";
      }

      public MetaBuilder()
      {
         _meta = new Thrift.FileMetaData();

         _meta.Created_by = CreatedBy;
         _meta.Version = 1;
         _meta.Row_groups = new List<Thrift.RowGroup>();
      }

      public Thrift.FileMetaData ThriftMeta => _meta;

      public void AddSchema(DataSet ds)
      {
         ds.Metadata.CreatedBy = CreatedBy;
         _meta.Schema = new List<TSchemaElement> { new TSchemaElement("schema") { Num_children = ds.Schema.Elements.Count } };
         _meta.Schema.AddRange(ds.Schema.Elements.Select(c => c.Thrift));
         _meta.Num_rows = ds.Count;
      }

      public void SetMeta(Thrift.FileMetaData meta)
      {
         _meta = meta;
      }

      public Thrift.RowGroup AddRowGroup()
      {
         var rg = new Thrift.RowGroup();
         _meta.Row_groups.Add(rg);
         return rg;
      }

      public Thrift.ColumnChunk AddColumnChunk(CompressionMethod compression, Stream output, SchemaElement schema, int valuesCount)
      {
         Thrift.CompressionCodec codec = DataFactory.GetThriftCompression(compression);

         var chunk = new Thrift.ColumnChunk();
         long startPos = output.Position;
         chunk.File_offset = startPos;
         chunk.Meta_data = new Thrift.ColumnMetaData();
         chunk.Meta_data.Num_values = valuesCount;
         chunk.Meta_data.Type = schema.Thrift.Type;
         chunk.Meta_data.Codec = codec;
         chunk.Meta_data.Data_page_offset = startPos;
         chunk.Meta_data.Encodings = new List<Thrift.Encoding>
         {
            Thrift.Encoding.PLAIN
         };
         chunk.Meta_data.Path_in_schema = new List<string> { schema.Name };

         return chunk;
      }

      public Thrift.PageHeader CreateDataPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DATA_PAGE, 0, 0);
         ph.Data_page_header = new Thrift.DataPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Definition_level_encoding = Thrift.Encoding.RLE,
            Repetition_level_encoding = Thrift.Encoding.BIT_PACKED,
            Num_values = valueCount
         };

         return ph;
      }

      public Thrift.PageHeader CreateDictionaryPage(int valueCount)
      {
         var ph = new Thrift.PageHeader(Thrift.PageType.DICTIONARY_PAGE, 0, 0);
         ph.Dictionary_page_header = new Thrift.DictionaryPageHeader
         {
            Encoding = Thrift.Encoding.PLAIN,
            Is_sorted = false,
            Num_values = valueCount
         };
         return ph;
      }
   }
}
