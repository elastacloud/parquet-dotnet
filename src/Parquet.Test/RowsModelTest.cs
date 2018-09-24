using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   public class RowsModelTest : TestBase
   {
      #region [ Flat Tables ]

      [Fact]
      public void Flat_add_valid_row_succeeds()
      {
         var table = new Table(new Schema(new DataField<int>("id")));
         table.Add(new Row(1));
      }

      [Fact]
      public void Flat_add_invalid_type_fails()
      {
         var table = new Table(new Schema(new DataField<int>("id")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row("1")));
      }

      [Fact]
      public void Flat_write_read()
      {
         var table = new Table(new Schema(new DataField<int>("id"), new DataField<string>("city")));
         var ms = new MemoryStream();

         //generate fake data
         for (int i = 0; i < 1000; i++)
         {
            table.Add(new Row(i, "record#" + i));
         }

         //write to stream
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.True(table.Equals(table2, true));
      }

      #endregion


      #region [ Array Tables ]

      [Fact]
      public void Array_validate_succeeds()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         table.Add(new Row(new[] { 1, 2, 3 }));
         table.Add(new Row(new[] { 4, 5, 6 }));
      }

      [Fact]
      public void Array_validate_fails()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Array_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<int>("id"),
               new DataField<string[]>("categories")     //array field
               )
            );
         var ms = new MemoryStream();

         table.Add(1, new[] { "1", "2", "3" });
         table.Add(3, new[] { "3", "3", "3" });

         //write to stream
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.Equal(table.ToString(), table2.ToString());
      }

      #endregion

      #region [ Maps ]

      [Fact]
      public void Map_validate_succeeds()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         table.Add(Row.SingleCell(
            new List<Row>
            {
               new Row("one", "v1"),
               new Row("two", "v2")
            }));
      }

      [Fact]
      public void Map_validate_fails()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Map_read_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("map.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("[{1;[{1;one};{2;two};{3;three}]}]", t.ToString());
      }

      [Fact]
      public void Map_write_read()
      {
         var table = new Table(
            new Schema(
               new DataField<string>("city"),
               new MapField("population",
                  new DataField<int>("areaId"),
                  new DataField<long>("count"))));
         var ms = new MemoryStream();

         table.Add("London",
            new List<Row>
            {
               new Row(234, 100L),
               new Row(235, 110L)
            });

         //write as table
         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         //read back into table
         ms.Position = 0;
         Table table2;
         using (var reader = new ParquetReader(ms))
         {
            table2 = reader.ReadAsTable();
         }

         //validate data
         Assert.Equal(table.ToString(), table2.ToString());
      }

      #endregion

      #region [ Struct ]

      /*[Fact]
      public void List_validate_succeeds()
      {
         var table = new Table(new Schema(
            new DataField<int>("id"),
            new ListField("cities", new DataField<string>("item"))
            ));

         table.Add(new Row(1, new List<Row> { new Row("London"), new Row("New York") }));
         table.Add(new Row(2, new List<Row> { new Row("Birmingham"), new Row("Camden Town") }));
      }*/

      [Fact]
      public void Struct_read_plain_structs_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("struct_plain.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("[{12345-6;{Ivan;Gavryliuk}},{12345-7;{Richard;Conway}}]", t.ToString());
      }

      #endregion
   }
}