using Parquet.Data;
using System;
using Xunit;

namespace Parquet.Test
{
   public class DataSetTest
   {
      [Fact]
      public void Validate_cant_add_null_row()
      {
         var ds = new DataSet();
         Assert.Throws<ArgumentNullException>(() => ds.Add((Row)null));
      }

      [Fact]
      public void Validate_type_mismatch_throws_exception()
      {
         var ds = new DataSet(new SchemaElement<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add(4));
      }

      [Fact]
      public void Validate_wrongcolnumber_throws_exception()
      {
         var ds = new DataSet(new SchemaElement<string>("s"));

         Assert.Throws<ArgumentException>(() => ds.Add("1", 2));
      }

      [Fact]
      public void Check_datasetstats_numerical_max()
      {
         var ds = new DataSet(new SchemaElement<string>("s"), new SchemaElement<int>("i"),
            new SchemaElement<float>("f")) {{"1", 2, 3F}, {"1", 3, 4F}, {"1", 4, 5F}};

         Assert.Equal(4, ds.GetColumnStats(1).Max);
         Assert.Equal(5, ds.GetColumnStats(2).Max);
      }

      [Fact]
      public void Check_datasetstats_numerical_min()
      {
         var ds = new DataSet(new SchemaElement<string>("s"), new SchemaElement<int>("i"),
            new SchemaElement<float>("f")) { { "1", 2, 3F }, { "1", 3, 4F }, { "1", 4, 5F } };

         Assert.Equal(2, ds.GetColumnStats(1).Min);
         Assert.Equal(3, ds.GetColumnStats(2).Min);
      }

      [Fact]
      public void Check_datasetstats_numerical_mean()
      {
         var ds = new DataSet(new SchemaElement<string>("s"), new SchemaElement<int>("i"),
            new SchemaElement<float>("f")) { { "1", 2, 3.2F }, { "1", 3, 4F }, { "1", 4, 5F } };

         Assert.Equal(3, ds.GetColumnStats(1).Mean);
         Assert.Equal(4.07, Math.Round(ds.GetColumnStats(2).Mean, 2));
      }

      [Fact]
      public void Check_datasetstats_numerical_sd()
      {
         var ds = new DataSet(new SchemaElement<string>("s"), new SchemaElement<int>("i"),
            new SchemaElement<float>("f")) { { "1", 2, 3.2F }, { "1", 3, 4F }, { "1", 4, 5F } };

         Assert.Equal(1, ds.GetColumnStats(1).StandardDeviation);
         Assert.Equal(0.9, Math.Round(ds.GetColumnStats(2).StandardDeviation, 2));
      }

      [Fact]
      public void Check_datasetstats_numerical_nulls()
      {
         var ds = new DataSet(new SchemaElement<string>("s"), new SchemaElement<int>("i"),
            new SchemaElement<float>("f")) { { "1", 2, null }, { null, null, 4F }, { "1", 4, null } };

         Assert.Equal(1, ds.GetColumnStats(0).NullCount);
         Assert.Equal(0, ds.GetColumnStats(0).Mean);
         Assert.Equal(1, ds.GetColumnStats(1).NullCount);
         Assert.Equal(2, ds.GetColumnStats(2).NullCount);
      }


   }
}
