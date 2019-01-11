using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class SignedByteDataTypeHandler : BasicPrimitiveDataTypeHandler<sbyte>
   {
      public SignedByteDataTypeHandler(): base(DataType.SignedByte, Thrift.Type.INT32, Thrift.ConvertedType.INT_8)
      {

      }

      protected override sbyte ReadSingle(BinaryReader reader, Thrift.SchemaElement tse)
      {
         return reader.ReadSByte();
      }

      protected override void WriteOne(BinaryWriter writer, sbyte value)
      {
         writer.Write(value);
      }
   }
}
