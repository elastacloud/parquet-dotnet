using System;
using System.Collections.Generic;
using System.Linq;

namespace parq.Display.Views
{
   public class ConsoleView
    {
      public void Draw(ViewModel viewModel)
      {
         Console.Clear();
         var viewPort = new ViewPort();

         var chosenHeaders = viewModel.Columns;
         DrawLine(chosenHeaders);
         WriteHeaderLine(chosenHeaders);
         DrawLine(chosenHeaders);
         WriteValues(viewModel);
         DrawLine(chosenHeaders);
         WriteSummary(viewModel);
      }

      private void WriteSummary(ViewModel viewModel)
      {
         Console.WriteLine("{0} Rows Affected. Press ENTER to quit;", viewModel.RowCount);
      }
      private void WriteHeaderLine(IEnumerable<ColumnDetails> columnDetails)
      {
         Console.Write("|");
         foreach (string name in columnDetails.Select(cd => cd.columnName))
         {
            if (name.Length < AppSettings.Instance.DisplayMinWidth.Value)
            {
               for (int i = 0; i < AppSettings.Instance.DisplayMinWidth.Value - name.Length; i++)
               {
                  Console.Write(" ");
               }
            }
            Console.Write(name);
            Console.Write("|");
         }
         Console.Write(Environment.NewLine);
      }

      private void WriteValues(ViewModel viewModel)
      {
         for (int i = 0; i < viewModel.Rows.Count; i++)
         {
            var row = viewModel.Rows[i];
            Console.Write("|");
            for (int j = 0; j < row.Length; j++)
            {
               var header = viewModel.Columns.ElementAt(j);
               Console.Write(header.GetFormattedValue(row[j]));
               Console.Write("|");
            }
            Console.WriteLine();
         }
      }
      private void DrawLine(IEnumerable<ColumnDetails> columnHeaderSizes)
      {
         Console.Write("|");
         foreach (int item in columnHeaderSizes.Select(d => d.columnWidth))
         {
            for (int i = 0; i < item; i++)
            {
               Console.Write("-");
            }
            Console.Write("|");
         }
         Console.Write(Environment.NewLine);
      }
   }
}
