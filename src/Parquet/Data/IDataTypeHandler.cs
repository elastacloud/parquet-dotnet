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

      int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset, ParquetOptions formatOptions);

      void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values);

      Array GetArray(int minCount, bool rent, bool isNullable);

      Array MergeDictionary(Array src, Array dictionary, int[] indexes, int indexLength);

      Array PackDefinitions(Array data, int maxDefinitionLevel, out int[] definitions, out int defiintionsLength);

      Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel);
   }
}