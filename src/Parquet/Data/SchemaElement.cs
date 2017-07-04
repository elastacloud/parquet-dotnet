using Parquet.File;
using System;
using PSE = Parquet.Thrift.SchemaElement;

namespace Parquet.Data
{
   public class SchemaElement
   {
      internal SchemaElement(PSE thriftSchema)
      {
         Name = thriftSchema.Name;
         ThriftSchema = thriftSchema;
         ElementType = ListFactory.ToSystemType(thriftSchema);
         IsNullable = thriftSchema.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
      }

      public string Name { get; }

      public Type ElementType { get; }

      public bool IsNullable { get; }

      internal PSE ThriftSchema { get; set; }

      public override string ToString()
      {
         return $"{Name} ({ElementType}), nullable: {IsNullable}";
      }
   }
}