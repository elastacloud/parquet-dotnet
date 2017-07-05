using Parquet.File;
using System;
using PSE = Parquet.Thrift.SchemaElement;

namespace Parquet.Data
{
   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement
   {
      internal SchemaElement(PSE thriftSchema)
      {
         Name = thriftSchema.Name;
         ThriftSchema = thriftSchema;
         ElementType = ListFactory.ToSystemType(thriftSchema);
         IsNullable = thriftSchema.Repetition_type != Thrift.FieldRepetitionType.REQUIRED;
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; }

      /// <summary>
      /// Element type
      /// </summary>
      public Type ElementType { get; }

      /// <summary>
      /// Returns true if element can have null values
      /// </summary>
      public bool IsNullable { get; }

      internal PSE ThriftSchema { get; set; }

      /// <summary>
      /// Pretty prints
      /// </summary>
      public override string ToString()
      {
         return $"{Name} ({ElementType}), nullable: {IsNullable}";
      }
   }
}