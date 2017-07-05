using System;
using System.Collections.Generic;
using System.Text;
using Type = System.Type;
using TType = Parquet.Thrift.Type;
using System.Collections;
using Parquet.Thrift;
using System.Reflection;

namespace Parquet.File
{
   static class ListFactory
   {
      struct TypeTag
      {
         public TType PType;

         public ConvertedType? ConvertedType;

         public TypeTag(TType ptype, ConvertedType? convertedType)
         {
            PType = ptype;
            ConvertedType = convertedType;
         }
      }

      private static readonly Dictionary<Type, TypeTag> TypeToTag = new Dictionary<Type, TypeTag>
      {
         { typeof(int), new TypeTag(TType.INT32, null) },
         { typeof(bool), new TypeTag(TType.BOOLEAN, null) },
         { typeof(string), new TypeTag(TType.BYTE_ARRAY, ConvertedType.UTF8) }
      };

      public static IList Create(Type systemType, SchemaElement schemaToSet = null, bool nullable = false)
      {
         if (schemaToSet != null)
         {
            if (!TypeToTag.TryGetValue(systemType, out TypeTag tag))
               throw new NotSupportedException($"system type {systemType} is not supported");

            schemaToSet.Type = tag.PType;
            if (tag.ConvertedType != null)
               schemaToSet.Converted_type = tag.ConvertedType.Value;
         }

         //make the type nullable if it's not a class
         if(nullable)
         {
            if(!systemType.GetTypeInfo().IsClass)
            {
               systemType = typeof(Nullable<>).MakeGenericType(systemType);
            }
         }

         //create generic list instance
         Type listType = typeof(List<>);
         Type listGType = listType.MakeGenericType(systemType);
         return (IList)Activator.CreateInstance(listGType);
      }

      public static IList Create(SchemaElement schema, bool nullable = false)
      {
         Type t = ToSystemType(schema);
         return Create(t, null, nullable);
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
