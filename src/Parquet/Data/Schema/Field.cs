﻿using System;

namespace Parquet.Data
{

   /// <summary>
   /// Element of dataset's schema
   /// </summary>
   public abstract class Field
   {
      /// <summary>
      /// Type of schema in this field
      /// </summary>
      public SchemaType SchemaType { get; }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; private set; }

      /// <summary>
      /// Gets Parquet column path. For non-nested columns always equals to column <see cref="Name"/> otherwise contains
      /// a dot (.) separated path to the column within Parquet file. Note that this is a physical path which depends on field
      /// schema and you shouldn't build any reasonable business logic based on it.
      /// </summary>
      public string Path { get; internal set; }

      /// <summary>
      /// Maximum repetition level, do not use, unstable!
      /// </summary>
      internal int MaxRepetitionLevel { get; set; }

      /// <summary>
      /// Maximum definition level, do not use, unstable!
      /// </summary>
      internal int MaxDefinitionLevel { get; set; }

      /// <summary>
      /// Used internally for serialisation
      /// </summary>
      internal string ClrPropName { get; set; }

      internal virtual string PathPrefix { set { } }

      // not in use, to be exposed later
      internal Thrift.SchemaElement NativeSchema { get; set; }

      /// <summary>
      /// Constructs a field with only requiremd parameters
      /// </summary>
      /// <param name="name">Field name, required</param>
      /// <param name="schemaType">Type of schema of this field</param>
      protected Field(string name, SchemaType schemaType)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));

         if(Name.Contains(Schema.PathSeparator))
         {
            throw new ArgumentException($"'{Schema.PathSeparator}' is not allowed in field name as it's used in Apache Parquet format as field path separator.");
         }

         SchemaType = schemaType;
         Path = name;
      }

      #region [ Internal Helpers ]

      internal virtual void Assign(Field field)
      {
         //only used by some schema fields internally to help construct a field hierarchy
      }

      internal bool Equals(Thrift.SchemaElement tse)
      {
         if (ReferenceEquals(tse, null)) return false;

         return tse.Name == Name;
      }

      #endregion
   }
}
