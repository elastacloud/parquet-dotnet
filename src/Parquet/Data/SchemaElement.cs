﻿using Parquet.File;
using System;
using PSE = Parquet.Thrift.SchemaElement;

namespace Parquet.Data
{
   /// <summary>
   /// Element of dataset's schema
   /// <typeparamref name="T">Type of element in the column</typeparamref>
   /// </summary>
   public class SchemaElement<T> : SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="isNullable">When true, column can have null values</param>
      public SchemaElement(string name, bool isNullable) : base(name, typeof(T), isNullable)
      {

      }
   }


   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public class SchemaElement
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="SchemaElement"/> class.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="elementType">Type of the element in this column</param>
      /// <param name="isNullable">When true, column can have null values</param>
      public SchemaElement(string name, Type elementType, bool isNullable)
      {
         Name = name;
         ElementType = elementType;
         IsNullable = isNullable;
         ThriftSchema = new PSE(name)
         {
            Repetition_type = isNullable
               ? Thrift.FieldRepetitionType.OPTIONAL
               : Thrift.FieldRepetitionType.REQUIRED,
         };
         TypeFactory.AdjustSchema(ThriftSchema, elementType);
      }

      internal SchemaElement(PSE thriftSchema)
      {
         Name = thriftSchema.Name;
         ThriftSchema = thriftSchema;
         ElementType = TypeFactory.ToSystemType(thriftSchema);
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