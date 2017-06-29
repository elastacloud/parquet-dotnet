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

         public TypeTag(TType ptype, Func<IList> create)
         {
            PType = ptype;
            Create = create;
         }
      }

      private static readonly Dictionary<Type, TypeTag> TypeToTag = new Dictionary<Type, TypeTag>
      {
         { typeof(int), new TypeTag(TType.INT32, () => new List<int>()) },
         { typeof(bool), new TypeTag(TType.BOOLEAN, () => new List<bool>()) }
      };

      public static IList Create(Type systemType, SchemaElement schema)
      {
         if (!TypeToTag.TryGetValue(systemType, out TypeTag tag))
            throw new NotSupportedException($"system type {systemType} is not supported");

         schema.Type = tag.PType;
         return tag.Create();
      }
   }
}
