using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Views.Tablular;

namespace Parquet.CLI.Models.Tabular
{
   public interface ICellContent
   {
      string Value { get; set; }
      ConsoleColor? ForegroundColor { get; set; }
   }
   public class BasicCellContent : ICellContent
   {
      public string Value { get; set; }
      public ConsoleColor? ForegroundColor { get;  set; }
   }
   public class BreakingRuleContentArea : ICellContent
   {
      public string Value { get; set; }
      public ConsoleColor? ForegroundColor { get; set; }
   }
   public abstract class TableCell {
      public abstract int CellLineCount { get;  }
      public virtual ICellContent[] ContentAreas { get; set; }
      public virtual ICellContent[] GetCellContentByLineOrdinal(int ordinal)
      {
         return ContentAreas;
      }
      public virtual void Write(IConsoleOutputter consoleOutputter, ViewPort viewPort, int lineOrdinal, ColumnDetails columnConstraints)
      {
         foreach (ICellContent item in ContentAreas)
         {
            if (item.ForegroundColor.HasValue)
            {
               consoleOutputter.SetForegroundColor(item.ForegroundColor.Value);
            }

            WriteAllValues(consoleOutputter, viewPort, columnConstraints, lineOrdinal);
            consoleOutputter.ResetColor();
         }
      }

      public virtual void WriteAllValues(IConsoleOutputter consoleOutputter, ViewPort viewPort, ColumnDetails columnConstraints, int lineOrdinal, bool displayNulls = true, string verticalSeparator = "|", string truncationIdentifier = "...")
      {
         foreach (ICellContent item in this.GetCellContentByLineOrdinal(lineOrdinal))
         {
            string data = columnConstraints.GetFormattedValue(item.Value, displayNulls);

            if (IsOverlyLargeColumn(columnConstraints, viewPort, verticalSeparator))
            {
               if (data.Length > viewPort.Width)
               {
                  consoleOutputter.Write(data.Substring(0, viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length - truncationIdentifier.Length));

                  consoleOutputter.SetForegroundColor(ConsoleColor.Yellow);
                  consoleOutputter.BackgroundColor = ConsoleColor.Black;
                  consoleOutputter.Write(truncationIdentifier);
                  consoleOutputter.ResetColor();
               }
            }
            else if (data.Contains("[null]"))
            {
               consoleOutputter.SetForegroundColor(ConsoleColor.DarkGray);
               consoleOutputter.Write(data);
               consoleOutputter.ResetColor();
            }
            else
            {
               consoleOutputter.Write(data);
            }
         }

      }
      private bool IsOverlyLargeColumn(ColumnDetails column, ViewPort viewPort, string verticalSeparator)
      {
         return column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }
   }
   public class BasicTableCell : TableCell {
      public override int CellLineCount { get => 1;  }
   }
   public class MultilineTableCell : TableCell
   {
      public override int CellLineCount { get => this.ContentAreas.OfType<BreakingRuleContentArea>().Count() + 1; }
   }
   public class TableRow {
      public TableCell[] Cells { get; set; }
      public int MaxCellLineCount { get { return Cells.Max(c => c.CellLineCount); } }
   }
   public class DisplayTable
   {
      public DisplayTable()
      {
         Rows = new TableRow[0];
      }
      public ColumnDetails[] ColumnDetails { get; set; }
      public TableRow Header { get; set; }
      public TableRow[] Rows { get; set; }
   }
}
