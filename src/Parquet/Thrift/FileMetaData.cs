#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
/*
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System.Collections.Generic;
using System.Text;
using Thrift.Protocol;

namespace Parquet.Thrift
{

   /// <summary>
   /// Description for file metadata
   /// </summary>
   public class FileMetaData : TBase
   {
      private List<KeyValue> _key_value_metadata;
      private string _created_by;

      /// <summary>
      /// Version of this file *
      /// </summary>
      public int Version { get; set; }

      /// <summary>
      /// Parquet schema for this file.  This schema contains metadata for all the columns.
      /// The schema is represented as a tree with a single root.  The nodes of the tree
      /// are flattened to a list by doing a depth-first traversal.
      /// The column metadata contains the path in the schema for that column which can be
      /// used to map columns to nodes in the schema.
      /// The first element is the root *
      /// </summary>
      public List<SchemaElement> Schema { get; set; }

      /// <summary>
      /// Number of rows in this file *
      /// </summary>
      public long Num_rows { get; set; }

      /// <summary>
      /// Row groups in this file *
      /// </summary>
      public List<RowGroup> Row_groups { get; set; }

      /// <summary>
      /// Optional key/value metadata *
      /// </summary>
      public List<KeyValue> Key_value_metadata
      {
         get
         {
            return _key_value_metadata;
         }
         set
         {
            __isset.key_value_metadata = true;
            this._key_value_metadata = value;
         }
      }

      /// <summary>
      /// String for application that wrote this file.  This should be in the format
      /// [Application] version [App Version] (build [App Build Hash]).
      /// e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
      /// </summary>
      public string Created_by
      {
         get
         {
            return _created_by;
         }
         set
         {
            __isset.created_by = true;
            this._created_by = value;
         }
      }


      public Isset __isset;



      public struct Isset
      {
         public bool key_value_metadata;
         public bool created_by;
      }

      public FileMetaData()
      {
         this.Row_groups = new List<RowGroup>();
      }

      public FileMetaData(int version, List<SchemaElement> schema, long num_rows, List<RowGroup> row_groups) : this()
      {
         this.Version = version;
         this.Schema = schema;
         this.Num_rows = num_rows;
         this.Row_groups = row_groups;
      }

      public void Read(TProtocol iprot)
      {
         iprot.IncrementRecursionDepth();
         try
         {
            bool isset_version = false;
            bool isset_schema = false;
            bool isset_num_rows = false;
            bool isset_row_groups = false;
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
                        Version = iprot.ReadI32();
                        isset_version = true;
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
                           Schema = new List<SchemaElement>();
                           TList _list24 = iprot.ReadListBegin();
                           for (int _i25 = 0; _i25 < _list24.Count; ++_i25)
                           {
                              SchemaElement _elem26;
                              _elem26 = new SchemaElement();
                              _elem26.Read(iprot);
                              Schema.Add(_elem26);
                           }
                           iprot.ReadListEnd();
                        }
                        isset_schema = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }
                     break;
                  case 3:
                     if (field.Type == TType.I64)
                     {
                        Num_rows = iprot.ReadI64();
                        isset_num_rows = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }
                     break;
                  case 4:
                     if (field.Type == TType.List)
                     {
                        {
                           Row_groups = new List<RowGroup>();
                           TList _list27 = iprot.ReadListBegin();
                           for (int _i28 = 0; _i28 < _list27.Count; ++_i28)
                           {
                              RowGroup _elem29;
                              _elem29 = new RowGroup();
                              _elem29.Read(iprot);
                              Row_groups.Add(_elem29);
                           }
                           iprot.ReadListEnd();
                        }
                        isset_row_groups = true;
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }
                     break;
                  case 5:
                     if (field.Type == TType.List)
                     {
                        {
                           Key_value_metadata = new List<KeyValue>();
                           TList _list30 = iprot.ReadListBegin();
                           for (int _i31 = 0; _i31 < _list30.Count; ++_i31)
                           {
                              KeyValue _elem32;
                              _elem32 = new KeyValue();
                              _elem32.Read(iprot);
                              Key_value_metadata.Add(_elem32);
                           }
                           iprot.ReadListEnd();
                        }
                     }
                     else
                     {
                        TProtocolUtil.Skip(iprot, field.Type);
                     }
                     break;
                  case 6:
                     if (field.Type == TType.String)
                     {
                        Created_by = iprot.ReadString();
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
            if (!isset_version)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_schema)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_num_rows)
               throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_row_groups)
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
            TStruct struc = new TStruct("FileMetaData");
            oprot.WriteStructBegin(struc);
            TField field = new TField();
            field.Name = "version";
            field.Type = TType.I32;
            field.ID = 1;
            oprot.WriteFieldBegin(field);
            oprot.WriteI32(Version);
            oprot.WriteFieldEnd();
            field.Name = "schema";
            field.Type = TType.List;
            field.ID = 2;
            oprot.WriteFieldBegin(field);
            {
               oprot.WriteListBegin(new TList(TType.Struct, Schema.Count));
               foreach (SchemaElement _iter33 in Schema)
               {
                  _iter33.Write(oprot);
               }
               oprot.WriteListEnd();
            }
            oprot.WriteFieldEnd();
            field.Name = "num_rows";
            field.Type = TType.I64;
            field.ID = 3;
            oprot.WriteFieldBegin(field);
            oprot.WriteI64(Num_rows);
            oprot.WriteFieldEnd();
            field.Name = "row_groups";
            field.Type = TType.List;
            field.ID = 4;
            oprot.WriteFieldBegin(field);
            {
               oprot.WriteListBegin(new TList(TType.Struct, Row_groups.Count));
               foreach (RowGroup _iter34 in Row_groups)
               {
                  _iter34.Write(oprot);
               }
               oprot.WriteListEnd();
            }
            oprot.WriteFieldEnd();
            if (Key_value_metadata != null && __isset.key_value_metadata)
            {
               field.Name = "key_value_metadata";
               field.Type = TType.List;
               field.ID = 5;
               oprot.WriteFieldBegin(field);
               {
                  oprot.WriteListBegin(new TList(TType.Struct, Key_value_metadata.Count));
                  foreach (KeyValue _iter35 in Key_value_metadata)
                  {
                     _iter35.Write(oprot);
                  }
                  oprot.WriteListEnd();
               }
               oprot.WriteFieldEnd();
            }
            if (Created_by != null && __isset.created_by)
            {
               field.Name = "created_by";
               field.Type = TType.String;
               field.ID = 6;
               oprot.WriteFieldBegin(field);
               oprot.WriteString(Created_by);
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
         StringBuilder __sb = new StringBuilder("FileMetaData(");
         __sb.Append(", Version: ");
         __sb.Append(Version);
         __sb.Append(", Schema: ");
         __sb.Append(Schema);
         __sb.Append(", Num_rows: ");
         __sb.Append(Num_rows);
         __sb.Append(", Row_groups: ");
         __sb.Append(Row_groups);
         if (Key_value_metadata != null && __isset.key_value_metadata)
         {
            __sb.Append(", Key_value_metadata: ");
            __sb.Append(Key_value_metadata);
         }
         if (Created_by != null && __isset.created_by)
         {
            __sb.Append(", Created_by: ");
            __sb.Append(Created_by);
         }
         __sb.Append(")");
         return __sb.ToString();
      }

   }

}
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member