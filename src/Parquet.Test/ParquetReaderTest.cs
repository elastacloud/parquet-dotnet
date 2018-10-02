﻿using NetBox;
using NetBox.IO;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Xunit;
using System.Linq;
using NetBox.Extensions;
using NetBox.Generator;

namespace Parquet.Test
{
   public class ParquetReaderTest : TestBase
   {
      [Fact]
      public void Opening_null_stream_fails()
      {
         Assert.Throws<ArgumentNullException>(() => new ParquetReader(null));
      }

      [Fact]
      public void Opening_small_file_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("small".ToMemoryStream()));
      }

      [Fact]
      public void Opening_file_without_proper_head_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("PAR2dataPAR1".ToMemoryStream()));
      }

      [Fact]
      public void Opening_file_without_proper_tail_fails()
      {
         Assert.Throws<IOException>(() => new ParquetReader("PAR1dataPAR2".ToMemoryStream()));
      }

      [Fact]
      public void Opening_readable_but_not_seekable_stream_fails()
      {
         Assert.Throws<ArgumentException>(() => new ParquetReader(new ReadableNonSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
      }

      [Fact]
      public void Opening_not_readable_but_seekable_stream_fails()
      {
         Assert.Throws<ArgumentException>(() => new ParquetReader(new NonReadableSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
      }

      /*[Fact]
      public void Read_from_offset_in_first_chunk()
      {
         DataSet ds = DataSetGenerator.Generate(30);
         var wo = new WriterOptions { RowGroupsSize = 5 };
         var ro = new ReaderOptions { Offset = 0, Count = 2 };

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms, CompressionMethod.None, null, wo);

         ms.Position = 0;
         DataSet ds1 = ParquetReader2.Read(ms, null, ro);

         Assert.Equal(30, ds1.TotalRowCount);
         Assert.Equal(2, ds1.RowCount);
         Assert.Equal(0, ds[0][0]);
         Assert.Equal(1, ds[1][0]);
      }

      [Fact]
      public void Read_from_offset_in_second_chunk()
      {
         DataSet ds = DataSetGenerator.Generate(15);
         var wo = new WriterOptions { RowGroupsSize = 5 };
         var ro = new ReaderOptions { Offset = 5, Count = 2 };

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms, CompressionMethod.None, null, wo);

         ms.Position = 0;
         DataSet ds1 = ParquetReader2.Read(ms, null, ro);

         Assert.Equal(15, ds1.TotalRowCount);
         Assert.Equal(2, ds1.RowCount);
         Assert.Equal(5, ds1[0][0]);
         Assert.Equal(6, ds1[1][0]);
      }

      [Fact]
      public void Read_from_offset_across_chunks()
      {
         DataSet ds = DataSetGenerator.Generate(15);
         var wo = new WriterOptions { RowGroupsSize = 5 };
         var ro = new ReaderOptions { Offset = 4, Count = 2 };

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms, CompressionMethod.None, null, wo);

         ms.Position = 0;
         DataSet ds1 = ParquetReader2.Read(ms, null, ro);

         Assert.Equal(15, ds1.TotalRowCount);
         Assert.Equal(2, ds1.RowCount);
         Assert.Equal(4, ds1[0][0]);
         Assert.Equal(5, ds1[1][0]);
      }

      [Fact]
      public void Read_from_negative_offset_fails()
      {
         DataSet ds = DataSetGenerator.Generate(15);
         var wo = new WriterOptions { RowGroupsSize = 5 };
         var ro = new ReaderOptions { Offset = -4, Count = 2 };

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms, CompressionMethod.None, null, wo);

         ms.Position = 0;
         Assert.Throws<ParquetException>(() => ParquetReader2.Read(ms, null, ro));
      }

      [Fact]
      public void Reads_created_by_metadata()
      {
         DataSet ds = DataSetGenerator.Generate(10);

         var ms = new MemoryStream();
         ParquetWriter2.Write(ds, ms);

         ms.Position = 0;
         DataSet ds1 = ParquetReader2.Read(ms);
         Assert.StartsWith("Parquet.Net", ds1.Metadata.CreatedBy);
      }

      //this only tests that the file is readable as it used to completely crash before
      [Fact]
      public void Reads_compat_nation_impala_file()
      {
         DataSet nation = ParquetReader2.Read(OpenTestFile("nation.impala.parquet"));

         Assert.Equal(25, nation.RowCount);
      }*/


      //[Fact]
      //public void Reads_really_mad_nested_file()
      //{
         /* Spark schema:
root
|-- addresses: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- line1: string (nullable = true)
|    |    |-- name: string (nullable = true)
|    |    |-- openingHours: array (nullable = true)
|    |    |    |-- element: long (containsNull = true)
|    |    |-- postcode: string (nullable = true)
|-- cities: array (nullable = true)
|    |-- element: string (containsNull = true)
|-- comment: string (nullable = true)
|-- id: long (nullable = true)
|-- location: struct (nullable = true)
|    |-- latitude: double (nullable = true)
|    |-- longitude: double (nullable = true)
|-- price: struct (nullable = true)
|    |-- lunch: struct (nullable = true)
|    |    |-- max: long (nullable = true)
|    |    |-- min: long (nullable = true) 
         */

         /*DataSet ds = ParquetReader2.Read(OpenTestFile("nested.parquet"));

         //much easier to compare mad nestness with .ToString(), but will break when it changes
         Assert.Equal("{[{Dante Road;Head Office;[9;10;11;12;13;14;15;16;17;18];SE11};{Somewhere Else;Small Office;[6;7;19;20;21;22;23];TN19}];[London;Derby];this file contains all the permunations for nested structures and arrays to test Parquet parser;1;{51.2;66.3};{{2;1}}}", ds[0].ToString());
         Assert.Equal("{[{Dante Road;Head Office;[9;10;11;12;13;14;15;16;17;18];SE11};{Somewhere Else;Small Office;[6;7;19;20;21;22;23];TN19}];[London;Derby];this file contains all the permunations for nested structures and arrays to test Parquet parser;1;{51.2;66.3};{{2;1}}}", ds[1].ToString());*/
      //}

      /*[Fact]
      public void Reads_list_of_structures()
      {
         DataSet ds = ParquetReader2.Read(OpenTestFile("repeatedstruct.parquet"));

         Assert.Equal("{[{UK;London};{US;New York}];1}", ds[0].ToString());
      }*/

      [Fact]
      public void Read_simple_list_with_one_item()
      {
         /*
root
|-- cities: array (nullable = true)
|    |-- element: string (containsNull = true)
|-- id: long (nullable = true)
          */

         /*DataSet ds = ParquetReader2.Read(OpenTestFile("simplerepeated.parquet"));

         Assert.Equal(2, ds.Schema.Length);
         Assert.Equal(SchemaType.List, ds.Schema[0].SchemaType);
         Assert.Equal(SchemaType.Data, ds.Schema[1].SchemaType);

         Assert.Equal("cities", ds.Schema[0].Name);
         Assert.Equal("id", ds.Schema[1].Name);

         Assert.Equal(ds[0][0], new[] { "London", "Derby", "Paris", "New York" });
         Assert.Equal(1L, ds[0][1]);

         Assert.Equal("{[London;Derby;Paris;New York];1}", ds[0].ToString());*/
      }

      [Fact]
      public void Read_simple_structure()
      {
         /*
          * root
|-- city: struct (nullable = true)
|    |-- country: string (nullable = true)
|    |-- isCapital: boolean (nullable = true)
|    |-- name: string (nullable = true)
|-- id: long (nullable = true)
          */

         //Assert.Throws<NotSupportedException>(() => ParquetReader.Read(OpenTestFile("simplenested.parquet")));
         //return;

        /* DataSet ds = ParquetReader2.Read(OpenTestFile("simplenested.parquet"));

         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.FieldCount);

         Assert.Equal(SchemaType.Struct, ds.Schema[0].SchemaType);
         Assert.Equal(SchemaType.Data, ds.Schema[1].SchemaType);

         Assert.Equal("city", ds.Schema.FieldNames[0]);
         Assert.Equal("id", ds.Schema.FieldNames[1]);

         Row mr = ds[0];

         Row city = mr.Get<Row>(0);
         Assert.Equal("United Kingdom", city[0]);
         Assert.True((bool)city[1]);
         Assert.Equal("London", city[2]);

         Assert.Equal(1L, mr[1]);
         Assert.Equal("{{United Kingdom;True;London};1}", ds[0].ToString());*/
      }

      [Fact]
      public void Read_simple_map()
      {
         using (var reader = new ParquetReader(OpenTestFile("map_simple.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] data = reader.ReadEntireRowGroup();

            Assert.Equal(new int?[] { 1 }, data[0].Data);
            Assert.Equal(new int[] { 1, 2, 3 }, data[1].Data);
            Assert.Equal(new string[] { "one", "two", "three" }, data[2].Data);
         }
      }

      [Fact]
      public void Read_hardcoded_decimal()
      {
         using (var reader = new ParquetReader(OpenTestFile("complex-primitives.parquet")))
         {
            decimal value = (decimal)reader.ReadEntireRowGroup()[1].Data.GetValue(0);
            Assert.Equal((decimal)1.2, value);
         }
      }

      /*[Fact]
      public void Read_column_with_all_nulls()
      {
         var ds = new DataSet(new DataField<int?>("id"))
         {
            new object[] {null},
            new object[] {null}
         };

         DataSet ds1 = DataSetGenerator.WriteRead(ds);
      }

      [Fact]
      public void Read_all_nulls_no_booleans()
      {
         DataSet ds = ParquetReader2.Read(OpenTestFile("all_nulls_no_booleans.parquet"));
      }

      [Fact]
      public void Read_all_nulls_file()
      {
         DataSet ds = ParquetReader2.Read(OpenTestFile("all_nulls.parquet"));

         Assert.Equal(1, ds.Schema.Length);
         Assert.Equal("lognumber", ds.Schema[0].Name);
         Assert.Equal(1, ds.RowCount);
         Assert.Null(ds[0][0]);
      }

      [Fact]
      public void Read_all_nulls_decimal_column()
      {
         DataSet ds = ParquetReader2.Read(OpenTestFile("decimalnulls.parquet"));
      }

      [Fact]
      public void Read_all_legacy_decimals()
      {
         DataSet ds = ParquetReader2.Read(OpenTestFile("decimallegacy.parquet"));

         Row row = ds[0];
         Assert.Equal(1, (int)row[0]);
         Assert.Equal(1.2m, (decimal)row[1], 2);
         Assert.Null(row[2]);
         Assert.Equal(-1m, (decimal)row[3], 2);
      }


      [Fact]
      public void Read_only_limited_columns()
      {
         var options = new ReaderOptions
         {
            Columns = new[] { "n_name", "n_regionkey" }
         };

         DataSet ds = ParquetReader2.Read(OpenTestFile("nation.impala.parquet"), null, options);

         Assert.Equal(2, ds.FieldCount);
      }*/


      [Fact]
      public void Reads_multi_page_file()
      {
         using (var reader = new ParquetReader(OpenTestFile("multi.page.parquet"), leaveStreamOpen: false))
         {
            DataColumn[] data = reader.ReadEntireRowGroup();
            Assert.Equal(927861, data[0].Data.Length);

            int[] firstColumn = (int[])data[0].Data;
            Assert.Equal(30763, firstColumn[524286]);
            Assert.Equal(30766, firstColumn[524287]);

            //At row 524288 the data is split into another page
            //The column makes use of a dictionary to reduce the number of values and the default dictionary index value is zero (i.e. the first record value)
            Assert.NotEqual(firstColumn[0], firstColumn[524288]);

            //The vlaue should be 30768
            Assert.Equal(30768, firstColumn[524288]);
         }
      }

      class ReadableNonSeekableStream : DelegatedStream
      {
         public ReadableNonSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => false;

         public override bool CanRead => true;
      }

      class NonReadableSeekableStream : DelegatedStream
      {
         public NonReadableSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => true;

         public override bool CanRead => false;
      }

      class ReadableAndSeekableStream : DelegatedStream
      {
         public ReadableAndSeekableStream(Stream master) : base(master)
         {
         }

         public override bool CanSeek => true;

         public override bool CanRead => true;

      }
   }
}
