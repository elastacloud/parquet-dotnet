using System;
using System.Collections.Generic;
using System.Text;
using Parquet.CLI.Views.Tablular;

namespace Parquet.CLI.Models.Tabular
{
   public abstract class TableCell {
      public virtual string Content { get; set; }
      public virtual void Write(IConsoleOutputter consoleOutputter)
      {
         consoleOutputter.Write(Content);
      }
   }
   public class BasicTableCell : TableCell {  }
   public class TableRow { }
   public class DisplayTable
   {
      public ColumnDetails[] ColumnDetails { get; set; }
      public TableCell[] Header { get; set; }
      public TableRow[] Rows { get; set; }
   }
}
