using System;
using System.Collections.Generic;
using System.Text;
using Type = System.Type;
using TType = Parquet.Thrift.Type;
using System.Collections;
using Parquet.Thrift;

namespace Parquet.File
{
   static class ListFactory
   {
      struct TypeTag
      {
         public TType PType;

         public Func<IList> Create;

         public ConvertedType? ConvertedType;

         public TypeTag(TType ptype, Func<IList> create, ConvertedType? convertedType)
         {
            PType = ptype;
            Create = create;
            ConvertedType = convertedType;
         }
      }

      private static readonly Dictionary<Type, TypeTag> TypeToTag = new Dictionary<Type, TypeTag>
      {
         { typeof(int), new TypeTag(TType.INT32, () => new List<int>(), null) },
         { typeof(bool), new TypeTag(TType.BOOLEAN, () => new List<bool>(), null) },
         { typeof(string), new TypeTag(TType.BYTE_ARRAY, () => new List<string>(), ConvertedType.UTF8) }
      };

      public static IList Create(Type systemType, SchemaElement schema = null)
      {
         if (!TypeToTag.TryGetValue(systemType, out TypeTag tag))
            throw new NotSupportedException($"system type {systemType} is not supported");

         if (schema != null)
         {
            schema.Type = tag.PType;
            if (tag.ConvertedType != null)
               schema.Converted_type = tag.ConvertedType.Value;
         }


         return tag.Create();
      }

      public static IList Create(SchemaElement schema)
      {
         Type t = ToSystemType(schema);
         return Create(t);
      }

      public static Type ToSystemType(SchemaElement schema)
      {
         switch (schema.Type)
         {
            case TType.BOOLEAN:
               return typeof(bool);
            case TType.INT32:
               if (schema.Converted_type == ConvertedType.DATE)
               {
                  return typeof(DateTimeOffset);
               }
               else
               {
                  return typeof(int);
               }
            case TType.FLOAT:
               return typeof(float);
            case TType.INT64:
               return typeof(long);
            case TType.DOUBLE:
               return typeof(double);
            case TType.INT96:
               return typeof(DateTimeOffset);
            case TType.BYTE_ARRAY:
               if (schema.Converted_type == ConvertedType.UTF8)
               {
                  return typeof(string);
               }
               else
               {
                  return typeof(byte[]);
               }
            case TType.FIXED_LEN_BYTE_ARRAY:
               if (schema.Converted_type == ConvertedType.DECIMAL)
               {
                  return typeof(decimal);
               }
               else
               {
                  return typeof(byte[]);
               }
            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }

      }
   }
}
