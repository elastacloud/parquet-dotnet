using System;
using System.Collections.Generic;
using System.Text;
using Cpf.Widgets;
using static Cpf.PoshConsole;
using NetBox.Extensions;
using System.Linq;

namespace Parquet.CLI.Commands
{
   class MetaCommand : FileInputCommand
   {
      public MetaCommand(string path) : base(path)
      {
         Telemetry.CommandExecuted("meta",
            "path", path);
      }

      public void Execute()
      {
         Thrift.FileMetaData fileMeta = ReadInternalMetadata();

         //root metadata
         WriteLine("File Metadata", T.HeadingTextColor);
         var t = new Table("name", "value");
         t.AddRow("Created By", fileMeta.Created_by);
         t.AddRow("Total Rows", fileMeta.Num_rows);
         t.AddRow("Version", fileMeta.Version);
         t.Render(false, T.HeadingTextColor, T.NormalTextColor);
         WriteLine();

         //custom key-values
         if (fileMeta.Key_value_metadata == null || fileMeta.Key_value_metadata.Count == 0)
         {
            WriteLine("no custom key-value metadata", ConsoleColor.Gray);
         }
         else
         {
            WriteLine("Key-Value Metadata", T.HeadingTextColor);
            t = new Table("key", "vlaue");
            foreach(Thrift.KeyValue kv in fileMeta.Key_value_metadata)
            {
               t.AddRow(kv.Key, kv.Value);
            }
            t.Render(false, T.HeadingTextColor, T.NormalTextColor);
         }
         WriteLine();

         //row groups
         WriteLine("Row Groups", T.HeadingTextColor);
         int i = 0;
         foreach(Thrift.RowGroup rg in fileMeta.Row_groups)
         {
            Print(rg, i++);
         }
      }

      private void Print(Thrift.RowGroup rg, int index)
      {
         WriteLine();
         PoshWriteLine($"Row Group #{{{index+1}}}", ConsoleColor.Red);
         var t = new Table("name", "value");
         t.AddRow("Total Rows", rg.Num_rows);
         t.AddRow("Total Byte Size", GetSizeString(rg.Total_byte_size));
         t.Render(false, T.HeadingTextColor, T.NormalTextColor);

         //columns
         int i = 0;
         foreach(Thrift.ColumnChunk column in rg.Columns)
         {
            t = new Table("name", "value");
            t.AddRow("File Offset", column.File_offset);
            t.AddRow("File Path", column.File_path ?? string.Empty);
            t.AddRow("Codec", column.Meta_data.Codec);
            t.AddRow("Data Page Offset", column.Meta_data.Data_page_offset);
            t.AddRow("Dictionary Page Offset", column.Meta_data.Dictionary_page_offset);
            t.AddRow("Index Page Offset", column.Meta_data.Index_page_offset);
            t.AddRow("Encodings", string.Join(", ", column.Meta_data.Encodings));
            //t.AddRow("", column.Meta_data.Encoding_stats[0].)
            t.AddRow("Total Values", column.Meta_data.Num_values);
            t.AddRow("Path in Schema", string.Join(".", column.Meta_data.Path_in_schema));
            t.AddRow("Compressed Size", GetSizeString(column.Meta_data.Total_compressed_size));
            t.AddRow("Uncompressed Size", GetSizeString(column.Meta_data.Total_uncompressed_size));
            t.AddRow("Type", column.Meta_data.Type);
            t.AddRow("Statistics", GetStatsString(column.Meta_data.Statistics));
            PoshWriteLine($"Column #{{{i++}}}", ConsoleColor.Red);
            t.Render(false, T.HeadingTextColor, T.NormalTextColor);
            WriteLine();
         }
      }

      private string GetSizeString(long size)
      {
         return $"{size} ({size.ToFileSizeString()})";
      }

      private string GetStatsString(Thrift.Statistics stats)
      {
         var sb = new StringBuilder();

         sb.Append("nulls: ");
         if (stats.__isset.null_count)
            sb.Append(stats.Null_count);
         else
            sb.Append("?");

         sb.Append(", distincts: ");
         if (stats.__isset.distinct_count)
            sb.Append(stats.Distinct_count);
         else sb.Append("?");

         sb.Append(", min: ");
         if (stats.__isset.min)
            sb.Append("(hex) " + stats.Min.ToHexString());
         else
            sb.Append("?");

         sb.Append(", max: ");
         if (stats.__isset.max)
            sb.Append("(hex) " + stats.Max.ToHexString());
         else
            sb.Append("?");

         return sb.ToString();
      }
   }
}
