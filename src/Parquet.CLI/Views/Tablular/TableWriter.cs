using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Parquet.CLI.Models;
using Parquet.CLI.Models.Tabular;

namespace Parquet.CLI.Views.Tablular
{
   public interface IConsoleOutputter
   {
      void Write(string s);
      void WriteLine();
      ConsoleColor ForegroundColor { get; set; }
      ConsoleColor BackgroundColor { get; set; }
      void ResetColor();
   }

   public class TableWriter
   {
      private const string verticalSeparator = "|";
      private const string horizontalSeparator = "-";
      private const string cellDivider = "+";
      private readonly IConsoleOutputter consoleOutputter;
      private readonly ViewPort viewPort;

      public TableWriter(IConsoleOutputter consoleOutputter, ViewPort viewPort)
      {
         this.consoleOutputter = consoleOutputter;
         this.viewPort = viewPort;
      }
      private bool IsOverlyLargeColumn(ColumnDetails column, ViewPort viewPort)
      {
         return column.columnWidth + verticalSeparator.Length > viewPort.Width;
      }

      public void Draw(DisplayTable table)
      {
         if (table.Header.Any())
         {
            DrawLine(table, viewPort);
            WriteHeaderLine(table, viewPort);
            DrawLine(table, viewPort);
         }
      }

      private void WriteHeaderLine(DisplayTable table, ViewPort viewPort, bool displayTypes=false)
      {
         consoleOutputter.Write(verticalSeparator);
         for (int h = 0; h < table.Header.Length; h++)
         {
            ColumnDetails column = table.ColumnDetails[h];
            TableCell cell = table.Header[h];
            if (IsOverlyLargeColumn(column, viewPort))
            {
               for (int i = 0; i < viewPort.Width - cell.Content.Length - (verticalSeparator.Length * 2) - Environment.NewLine.Length; i++)
               {
                  consoleOutputter.Write(" ");
               }
            }
            else
            {
               for (int i = 0; i < column.columnWidth - cell.Content.Length; i++)
               {
                  consoleOutputter.Write(" ");
               }
            }

            consoleOutputter.Write(cell.Content);
            consoleOutputter.Write(verticalSeparator);
         }
         consoleOutputter.Write(Environment.NewLine);

         // todo: the idea of a TableCell is to handle this
         /*if (displayTypes)
         {
            consoleOutputter.Write(verticalSeparator);
            foreach (ColumnDetails column in sheet.Columns)
            {
               int offset = column.isNullable ? 1 : 0;
               if (IsOverlyLargeColumn(column, viewPort))
               {
                  for (int i = 0; i < viewPort.Width - offset - column.type.ToString().Length - (verticalSeparator.Length * 2) - Environment.NewLine.Length; i++)
                  {
                     consoleOutputter.Write(" ");
                  }
               }
               else
               {
                  for (int i = 0; i < column.columnWidth - column.type.ToString().Length - offset; i++)
                  {
                     consoleOutputter.Write(" ");
                  }
               }
               consoleOutputter.ForegroundColor = ConsoleColor.Blue;

               if (column.columnWidth > column.type.ToString().Length + (verticalSeparator.Length * 2))
               {
                  consoleOutputter.Write(column.type.ToString());
               }
               else
               {
                  int howMuchSpaceDoWeHave = column.isNullable ? column.columnWidth - 1 : column.columnWidth;
                  consoleOutputter.Write(column.type.ToString().Substring(0, howMuchSpaceDoWeHave));
               }


               if (column.isNullable)
               {
                  consoleOutputter.ForegroundColor = ConsoleColor.Cyan;
                  consoleOutputter.Write("?");
                  consoleOutputter.ResetColor();
               }
               consoleOutputter.Write(verticalSeparator);
            }
            consoleOutputter.Write(Environment.NewLine);
         }*/
      }

      private void WriteValues(ViewModel viewModel, ConsoleSheet columnsFitToScreen, ConsoleFold foldedRows, ViewPort viewPort, bool displayNulls, string truncationIdentifier, bool displayRefs)
      {
         for (int i = foldedRows.IndexStart; i < foldedRows.IndexEnd; i++)
         {
            object[] row = viewModel.Rows.ElementAt(i);
            consoleOutputter.Write(verticalSeparator);
            for (int j = 0; j < row.Length; j++)
            {
               ColumnDetails header = viewModel.Columns.ElementAt(j);
               if (columnsFitToScreen.Columns.Any(x => x.columnName == header.columnName))
               {
                  ColumnDetails persistedFit = columnsFitToScreen.Columns.First(x => x.columnName == header.columnName);
                  string data = persistedFit.GetFormattedValue(row[j], displayNulls);

                  if (IsOverlyLargeColumn(persistedFit, viewPort))
                  {
                     if (data.Length > viewPort.Width)
                     {
                        consoleOutputter.Write(data.Substring(0, viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length - truncationIdentifier.Length));

                        consoleOutputter.ForegroundColor = ConsoleColor.Yellow;
                        consoleOutputter.BackgroundColor = ConsoleColor.Black;
                        consoleOutputter.Write(truncationIdentifier);
                        consoleOutputter.ResetColor();
                     }
                  }
                  else if (data.Contains("[null]"))
                  {
                     consoleOutputter.ForegroundColor = ConsoleColor.DarkGray;
                     consoleOutputter.Write(data);
                     consoleOutputter.ResetColor();
                  }
                  else
                  {
                     consoleOutputter.Write(data);
                  }

                  consoleOutputter.Write(verticalSeparator);
               }
            }
            consoleOutputter.WriteLine();
         }
      }
      private void DrawLine(DisplayTable displayTable, ViewPort viewPort, bool drawRefs = false)
      {
         consoleOutputter.Write(cellDivider);
         for (int c = 0; c < displayTable.Header.Length; c++)
         {
            TableCell column = displayTable.Header[c];
            ColumnDetails details = displayTable.ColumnDetails[c];

            int columnNameLength = c.ToString().Length;
            if (IsOverlyLargeColumn(details, viewPort))
            {
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  consoleOutputter.Write(c.ToString());
               }
               else
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               for (int i = 0; i < ((viewPort.Width - (verticalSeparator.Length * 2) - Environment.NewLine.Length) / 2) - (columnNameLength / 2) - ((columnNameLength + 1) % 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
            }
            else
            {
               for (int i = 0; i < (details.columnWidth / 2) - (c.ToString().Length / 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               if (drawRefs)
               {
                  consoleOutputter.Write(c.ToString());
               }
               else
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
               for (int i = 0; i < (details.columnWidth / 2) - (c.ToString().Length / 2) - ((details.columnWidth + 1) % 2); i++)
               {
                  consoleOutputter.Write(horizontalSeparator);
               }
            }
            consoleOutputter.Write(cellDivider);
         }
         consoleOutputter.Write(Environment.NewLine);
      }

      
   }
}
