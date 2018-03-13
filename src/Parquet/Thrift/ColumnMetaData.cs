/*
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
namespace Parquet.Thrift
{
   using System.Collections.Generic;
   using System.Text;
   using global::Thrift.Protocol;


   /// <summary>
   /// Description for column metadata
   /// </summary>
   class ColumnMetaData :
      TBase
   {
      List<KeyValue> _key_value_metadata;
      long _index_page_offset;
      long _dictionary_page_offset;
      Statistics _statistics;
      List<PageEncodingStats> _encoding_stats;

      /// <summary>
      /// Type of this column *
      /// 
      /// <seealso cref="Type"/>
      /// </summary>
      public Type Type { get; set; }

      /// <summary>
      /// Set of all encodings used for this column. The purpose is to validate
      /// whether we can decode those pages. *
      /// </summary>
      public List<Encoding> Encodings { get; set; }

      /// <summary>
      /// Path in schema *
      /// </summary>
      public List<string> Path_in_schema { get; set; }

      /// <summary>
      /// Compression codec *
      /// 
      /// <seealso cref="CompressionCodec"/>
      /// </summary>
      public CompressionCodec Codec { get; set; }

      /// <summary>
      /// Number of values in this column *
      /// </summary>
      public long Num_values { get; set; }

      /// <summary>
      /// total byte size of all uncompressed pages in this column chunk (including the headers) *
      /// </summary>
      public long Total_uncompressed_size { get; set; }

      /// <summary>
      /// total byte size of all compressed pages in this column chunk (including the headers) *
      /// </summary>
      public long Total_compressed_size { get; set; }

      /// <summary>
      /// Optional key/value metadata *
      /// </summary>
      public List<KeyValue> Key_value_metadata
      {
         get => _key_value_metadata;
         set
         {
            __isset.key_value_metadata = true;
            this._key_value_metadata = value;
         }
      }

      /// <summary>
      /// Byte offset from beginning of file to first data page *
      /// </summary>
      public long Data_page_offset { get; set; }

      /// <summary>
      /// Byte offset from beginning of file to root index page *
      /// </summary>
      public long Index_page_offset
      {
         get => _index_page_offset;
         set
         {
            __isset.index_page_offset = true;
            this._index_page_offset = value;
         }
      }

      /// <summary>
      /// Byte offset from the beginning of file to first (only) dictionary page *
      /// </summary>
      public long Dictionary_page_offset
      {
         get => _dictionary_page_offset;
         set
         {
            __isset.dictionary_page_offset = true;
            this._dictionary_page_offset = value;
         }
      }

      /// <summary>
      /// optional statistics for this column chunk
      /// </summary>
      public Statistics Statistics
      {
         get => _statistics;
         set
         {
            __isset.statistics = true;
            this._statistics = value;
         }
      }

      /// <summary>
      /// Set of all encodings used for pages in this column chunk.
      /// This information can be used to determine if all data pages are
      /// dictionary encoded for example *
      /// </summary>
      public List<PageEncodingStats> Encoding_stats
      {
         get => _encoding_stats;
         set
         {
            __isset.encoding_stats = true;
            this._encoding_stats = value;
         }
      }


      public Isset __isset;


      public struct Isset
      {
         public bool key_value_metadata;
         public bool index_page_offset;
         public bool dictionary_page_offset;
         public bool statistics;
         public bool encoding_stats;
      }

      public ColumnMetaData()
      {
      }

      public ColumnMetaData(Type type, List<Encoding> encodings, List<string> path_in_schema, CompressionCodec codec,
         long num_values, long total_uncompressed_size, long total_compressed_size, long data_page_offset) : this()
      {
         this.Type = type;
         this.Encodings = encodings;
         this.Path_in_schema = path_in_schema;
         this.Codec = codec;
         this.Num_values = num_values;
         this.Total_uncompressed_size = total_uncompressed_size;
         this.Total_compressed_size = total_compressed_size;
         this.Data_page_offset = data_page_offset;
      }

      public void Read(TProtocol iprot)
      {
         iprot.IncrementRecursionDepth();
         try
         {
            bool isset_type = false;
            bool isset_encodings = false;
            bool isset_path_in_schema = false;
            bool isset_codec = false;
            bool isset_num_values = false;
            bool isset_total_uncompressed_size = false;
            bool isset_total_compressed_size = false;
            bool isset_data_page_offset = false;
            TField field;
            iprot.ReadStructBegin();
            while (true)
            {
               field = iprot.ReadFieldBegin();
               if (field.Type == TType.Stop)
               {
                  break;
               }

               switch (field.ID)
               {
                  case 1:
                     if (field.Type == TType.I32)
                     {
                        Type = (Type) iprot.ReadI32();
                        isset_type = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 2:
                     if (field.Type == TType.List)
                     {
                        {
                           Encodings = new List<Encoding>();
                           TList _list0 = iprot.ReadListBegin();
                           for (int _i1 = 0; _i1 < _list0.Count; ++_i1)
                           {
                              Encoding _elem2;
                              _elem2 = (Encoding) iprot.ReadI32();
                              Encodings.Add(_elem2);
                           }

                           iprot.ReadListEnd();
                        }
                        isset_encodings = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 3:
                     if (field.Type == TType.List)
                     {
                        {
                           Path_in_schema = new List<string>();
                           TList _list3 = iprot.ReadListBegin();
                           for (int _i4 = 0; _i4 < _list3.Count; ++_i4)
                           {
                              string _elem5;
                              _elem5 = iprot.ReadString();
                              Path_in_schema.Add(_elem5);
                           }

                           iprot.ReadListEnd();
                        }
                        isset_path_in_schema = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 4:
                     if (field.Type == TType.I32)
                     {
                        Codec = (CompressionCodec) iprot.ReadI32();
                        isset_codec = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 5:
                     if (field.Type == TType.I64)
                     {
                        Num_values = iprot.ReadI64();
                        isset_num_values = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 6:
                     if (field.Type == TType.I64)
                     {
                        Total_uncompressed_size = iprot.ReadI64();
                        isset_total_uncompressed_size = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 7:
                     if (field.Type == TType.I64)
                     {
                        Total_compressed_size = iprot.ReadI64();
                        isset_total_compressed_size = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 8:
                     if (field.Type == TType.List)
                     {
                        {
                           Key_value_metadata = new List<KeyValue>();
                           TList _list6 = iprot.ReadListBegin();
                           for (int _i7 = 0; _i7 < _list6.Count; ++_i7)
                           {
                              KeyValue _elem8;
                              _elem8 = new KeyValue();
                              _elem8.Read(iprot);
                              Key_value_metadata.Add(_elem8);
                           }

                           iprot.ReadListEnd();
                        }
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 9:
                     if (field.Type == TType.I64)
                     {
                        Data_page_offset = iprot.ReadI64();
                        isset_data_page_offset = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 10:
                     if (field.Type == TType.I64)
                     {
                        Index_page_offset = iprot.ReadI64();
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 11:
                     if (field.Type == TType.I64)
                     {
                        Dictionary_page_offset = iprot.ReadI64();
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 12:
                     if (field.Type == TType.Struct)
                     {
                        Statistics = new Statistics();
                        Statistics.Read(iprot);
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  case 13:
                     if (field.Type == TType.List)
                     {
                        {
                           Encoding_stats = new List<PageEncodingStats>();
                           TList _list9 = iprot.ReadListBegin();
                           for (int _i10 = 0; _i10 < _list9.Count; ++_i10)
                           {
                              PageEncodingStats _elem11;
                              _elem11 = new PageEncodingStats();
                              _elem11.Read(iprot);
                              Encoding_stats.Add(_elem11);
                           }

                           iprot.ReadListEnd();
                        }
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }

                     break;
                  default:
                     TProtocolUtil.Skip(iprot, field.Type);
                     break;
               }

               iprot.ReadFieldEnd();
            }

            iprot.ReadStructEnd();
            if (!isset_type)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_encodings)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_path_in_schema)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_codec)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_num_values)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_total_uncompressed_size)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_total_compressed_size)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_data_page_offset)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
         }
         finally
         {
            iprot.DecrementRecursionDepth();
         }
      }

      public void Write(TProtocol oprot)
      {
         oprot.IncrementRecursionDepth();
         try
         {
            TStruct struc = new TStruct("ColumnMetaData");
            oprot.WriteStructBegin(struc);
            TField field = new TField();
            field.Name = "type";
            field.Type = TType.I32;
            field.ID = 1;
            oprot.WriteFieldBegin(field);
            oprot.WriteI32((int) Type);
            oprot.WriteFieldEnd();
            field.Name = "encodings";
            field.Type = TType.List;
            field.ID = 2;
            oprot.WriteFieldBegin(field);
            {
               oprot.WriteListBegin(new TList(TType.I32, Encodings.Count));
               foreach (Encoding _iter12 in Encodings)
               {
                  oprot.WriteI32((int) _iter12);
               }

               oprot.WriteListEnd();
            }
            oprot.WriteFieldEnd();
            field.Name = "path_in_schema";
            field.Type = TType.List;
            field.ID = 3;
            oprot.WriteFieldBegin(field);
            {
               oprot.WriteListBegin(new TList(TType.String, Path_in_schema.Count));
               foreach (string _iter13 in Path_in_schema)
               {
                  oprot.WriteString(_iter13);
               }

               oprot.WriteListEnd();
            }
            oprot.WriteFieldEnd();
            field.Name = "codec";
            field.Type = TType.I32;
            field.ID = 4;
            oprot.WriteFieldBegin(field);
            oprot.WriteI32((int) Codec);
            oprot.WriteFieldEnd();
            field.Name = "num_values";
            field.Type = TType.I64;
            field.ID = 5;
            oprot.WriteFieldBegin(field);
            oprot.WriteI64(Num_values);
            oprot.WriteFieldEnd();
            field.Name = "total_uncompressed_size";
            field.Type = TType.I64;
            field.ID = 6;
            oprot.WriteFieldBegin(field);
            oprot.WriteI64(Total_uncompressed_size);
            oprot.WriteFieldEnd();
            field.Name = "total_compressed_size";
            field.Type = TType.I64;
            field.ID = 7;
            oprot.WriteFieldBegin(field);
            oprot.WriteI64(Total_compressed_size);
            oprot.WriteFieldEnd();
            if (Key_value_metadata != null && __isset.key_value_metadata)
            {
               field.Name = "key_value_metadata";
               field.Type = TType.List;
               field.ID = 8;
               oprot.WriteFieldBegin(field);
               {
                  oprot.WriteListBegin(new TList(TType.Struct, Key_value_metadata.Count));
                  foreach (KeyValue _iter14 in Key_value_metadata)
                  {
                     _iter14.Write(oprot);
                  }

                  oprot.WriteListEnd();
               }
               oprot.WriteFieldEnd();
            }

            field.Name = "data_page_offset";
            field.Type = TType.I64;
            field.ID = 9;
            oprot.WriteFieldBegin(field);
            oprot.WriteI64(Data_page_offset);
            oprot.WriteFieldEnd();
            if (__isset.index_page_offset)
            {
               field.Name = "index_page_offset";
               field.Type = TType.I64;
               field.ID = 10;
               oprot.WriteFieldBegin(field);
               oprot.WriteI64(Index_page_offset);
               oprot.WriteFieldEnd();
            }

            if (__isset.dictionary_page_offset)
            {
               field.Name = "dictionary_page_offset";
               field.Type = TType.I64;
               field.ID = 11;
               oprot.WriteFieldBegin(field);
               oprot.WriteI64(Dictionary_page_offset);
               oprot.WriteFieldEnd();
            }

            if (Statistics != null && __isset.statistics)
            {
               field.Name = "statistics";
               field.Type = TType.Struct;
               field.ID = 12;
               oprot.WriteFieldBegin(field);
               Statistics.Write(oprot);
               oprot.WriteFieldEnd();
            }

            if (Encoding_stats != null && __isset.encoding_stats)
            {
               field.Name = "encoding_stats";
               field.Type = TType.List;
               field.ID = 13;
               oprot.WriteFieldBegin(field);
               {
                  oprot.WriteListBegin(new TList(TType.Struct, Encoding_stats.Count));
                  foreach (PageEncodingStats _iter15 in Encoding_stats)
                  {
                     _iter15.Write(oprot);
                  }

                  oprot.WriteListEnd();
               }
               oprot.WriteFieldEnd();
            }

            oprot.WriteFieldStop();
            oprot.WriteStructEnd();
         }
         finally
         {
            oprot.DecrementRecursionDepth();
         }
      }

      public override string ToString()
      {
         StringBuilder __sb = new StringBuilder("ColumnMetaData(");
         __sb.Append(", Type: ");
         __sb.Append(Type);
         __sb.Append(", Encodings: ");
         __sb.Append(Encodings);
         __sb.Append(", Path_in_schema: ");
         __sb.Append(Path_in_schema);
         __sb.Append(", Codec: ");
         __sb.Append(Codec);
         __sb.Append(", Num_values: ");
         __sb.Append(Num_values);
         __sb.Append(", Total_uncompressed_size: ");
         __sb.Append(Total_uncompressed_size);
         __sb.Append(", Total_compressed_size: ");
         __sb.Append(Total_compressed_size);
         if (Key_value_metadata != null && __isset.key_value_metadata)
         {
            __sb.Append(", Key_value_metadata: ");
            __sb.Append(Key_value_metadata);
         }

         __sb.Append(", Data_page_offset: ");
         __sb.Append(Data_page_offset);
         if (__isset.index_page_offset)
         {
            __sb.Append(", Index_page_offset: ");
            __sb.Append(Index_page_offset);
         }

         if (__isset.dictionary_page_offset)
         {
            __sb.Append(", Dictionary_page_offset: ");
            __sb.Append(Dictionary_page_offset);
         }

         if (Statistics != null && __isset.statistics)
         {
            __sb.Append(", Statistics: ");
            __sb.Append(Statistics == null ? "<null>" : Statistics.ToString());
         }

         if (Encoding_stats != null && __isset.encoding_stats)
         {
            __sb.Append(", Encoding_stats: ");
            __sb.Append(Encoding_stats);
         }

         __sb.Append(")");
         return __sb.ToString();
      }
   }
}
