using System;
using System.Linq;
using Moq;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;
using Parquet.CLI.Test.Helper;
using Parquet.CLI.Views.Tablular;
using Xunit;

namespace Parquet.CLI.Test
{
   public class TableWriterBodyTests
   {
      [Fact]
      public void TableWriterWritesSomething()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));

         var table = new DisplayTable() {
            Header = new TableRow { Cells = new[] { new BasicTableCell { CellLineCount = 1, ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } } ,
            ColumnDetails = new [] { new ColumnDetails { columnName = "harrumph", columnWidth = 10, isNullable = false, type = Data.DataType.String  } },
            Rows = new [] { new TableRow { Cells = new [] { new BasicTableCell { CellLineCount = 1, ContentAreas = new [] { new BasicCellContent { Value = "This is a test value."  } }  } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.True(!string.IsNullOrEmpty(s));
      }

      [Fact]
      public void TableWriterWritesHeader()
      {
         var mockOutputter = new Mock<IConsoleOutputter>();
         var target = new TableWriter(mockOutputter.Object, new ViewPort(100, 100));
         Guid expectedValue = Guid.NewGuid();

         var table = new DisplayTable()
         {
            Header = new TableRow { Cells = new[] { new BasicTableCell { CellLineCount = 1, ContentAreas = new[] { new BasicCellContent { Value = "TestCell" } } } } },
            ColumnDetails = new[] { new ColumnDetails { columnName = "harrumph", columnWidth = expectedValue.ToString().Length+2, isNullable = false, type = Data.DataType.String } },
            Rows = new[] { new TableRow { Cells = new[] { new BasicTableCell { CellLineCount = 1, ContentAreas = new[] { new BasicCellContent { Value = expectedValue.ToString() } } } } } }
         };

         target.Draw(table);
         string s = ConsoleOutputHelper.getConsoleOutput(mockOutputter.Invocations);
         Assert.Contains(expectedValue.ToString(), s);
      }
   }
}
