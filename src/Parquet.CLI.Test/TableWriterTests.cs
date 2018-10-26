using System;
using System.Linq;
using Moq;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;
using Parquet.CLI.Views.Tablular;
using Xunit;

namespace Parquet.CLI.Test
{
   public class TableWriterTests
   {
      [Fact]
      public void TableWriterWritesSomething()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable() {
            Header = new [] { new BasicTableCell { Content = "TestCell" } },
            ColumnDetails = new [] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String  } }
         };

         target.Draw(table);
         string s = getConsoleOutput(mockOutputter.Invocations);
         Assert.True(!string.IsNullOrEmpty(s));
      }

      [Fact]
      public void TableWriterWritesHeader()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new[] { new BasicTableCell { Content = "TestCell" } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         string s = getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains("TestCell", s);
      }



      [Fact]
      public void TableWriterWritesHeaderLines()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable()
         {
            Header = new[] { new BasicTableCell { Content = "TestCell" } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String } }
         };

         target.Draw(table);
         string s = getConsoleOutput(mockOutputter.Invocations);
         Assert.Equal(20, s.Count(c => c == '-'));
      }

      private string getConsoleOutput(IInvocationList invocations)
      {
         return string.Concat(invocations.Select(i => i.Arguments.OfType<string>().First()));
      }
   }
}
