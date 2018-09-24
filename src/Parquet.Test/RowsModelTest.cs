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
      [Fact]
      public void Add_valid_row_succeeds()
      {
         var table = new Table(new Schema(new DataField<int>("id")));
         table.Add(new Row(1));
      }

      [Fact]
      public void Add_invalid_type_fails()
      {
         var table = new Table(new Schema(new DataField<int>("id")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row("1")));
      }

      [Fact]
      public void Validate_array_succeeds()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         table.Add(new Row(new[] { 1, 2, 3 }));
         table.Add(new Row(new[] { 4, 5, 6 }));
      }

      [Fact]
      public void Validate_array_fails()
      {
         var table = new Table(new Schema(new DataField<IEnumerable<int>>("ids")));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Validate_map_succeeds()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         table.Add(new Row(
            new Dictionary<string, string>
            {
               ["one"] = "v1",
               ["two"] = "v2"
            }));
      }

      [Fact]
      public void Validate_map_fails()
      {
         var table = new Table(new Schema(
            new MapField("map", new DataField<string>("key"), new DataField<string>("value"))
            ));

         Assert.Throws<ArgumentException>(() => table.Add(new Row(1)));
      }

      [Fact]
      public void Read_map_file_from_Apache_Spark()
      {
         Table t;
         using (Stream stream = OpenTestFile("map.parquet"))
         {
            using (var reader = new ParquetReader(stream))
            {
               t = reader.ReadAsTable();
            }
         }

         Assert.Equal("{1;[1=>one;2=>two;3=>three]}", t.ToString());
      }

      [Fact]
      public void Write_read_flat()
      {
         var table = new Table(new Schema(new DataField<int>("id"), new DataField<string>("city")));
         var ms = new MemoryStream();

         //generate fake data
         for(int i = 0; i < 1000; i++)
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

      [Fact]
      public void Write_read_array()
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
   }
}