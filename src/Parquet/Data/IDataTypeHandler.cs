﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Data
{
   interface IDataTypeHandler
   {
      /// <summary>
      /// Called by the library to determine if this data handler can be used in current schema position
      /// </summary>
      /// <param name="tse"></param>
      /// <param name="formatOptions"></param>
      /// <returns></returns>
      bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

      Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount);

      void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container);

      DataType DataType { get; }

      SchemaType SchemaType { get; }

      Type ClrType { get; }

      int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset);

      /// <summary>
      /// Reads a single value from the current stream position
      /// </summary>
      //object Read(BinaryReader reader, Thrift.SchemaElement tse, ParquetOptions formatOptions);

      void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics);

      /// <summary>
      /// Creates or rents a native array
      /// </summary>
      /// <param name="minCount">Minimum element count. Realistically there could be more elements than you've asked for only when arrays are rented.</param>
      /// <param name="rent">Rent or create</param>
      /// <param name="isNullable">Nullable elements or not</param>
      /// <returns></returns>
      Array GetArray(int minCount, bool rent, bool isNullable);

      Array MergeDictionary(Array dictionary, int[] indexes);

      Array PackDefinitions(Array data, int maxDefinitionLevel, out int[] definitions, out int defiintionsLength, out int nullCount);

      Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags);
   }
}