using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using TType = Parquet.Thrift.Type;

namespace Parquet.File.Values
{
   //see https://github.com/Parquet/parquet-format/blob/master/Encodings.md#plain-plain--0
   class PlainValuesWriter : IValuesWriter
   {
      public void Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
         switch (schema.Type)
         {
            case TType.BOOLEAN:
               WriteBoolean(writer, schema, data);
               break;

            case TType.INT32:
               WriteInt32(writer, schema, data);
               break;

            case TType.FLOAT:
               WriteFloat(writer, schema, data);
               break;

            case TType.INT64:
               WriteLong(writer, schema, data);
               break;

            case TType.DOUBLE:
               WriteDouble(writer, schema, data);
               break;

            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteBoolean(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<bool>)data;
         int n = 0;
         byte b = 0;
         byte[] buffer = new byte[data.Count / 8 + 1];
         int ib = 0;

         foreach (bool flag in data)
         {
            byte mask = (byte)(1 << n++);

            if (flag)
               b |= mask;
            else
               b &= mask;

            if (n == 8)
            {
               buffer[ib++] = b;
               n = 0;
               b = 0;
            }
         }

         if (n != 0) buffer[ib] = b;

         writer.Write(buffer);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteInt32(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var dataTyped = (List<int>)data;
         foreach (int el in dataTyped)
         {
            writer.Write(el);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteFloat(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<float>)data;
         foreach (float f in lst)
         {
            writer.Write(f);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteLong(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<long>)data;
         foreach (long l in lst)
         {
            writer.Write(l);
         }
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      private static void WriteDouble(BinaryWriter writer, SchemaElement schema, IList data)
      {
         var lst = (List<double>)data;
         foreach (float d in lst)
         {
            writer.Write(d);
         }
      }


   }
}
