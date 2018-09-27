﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cpf.Widgets;
using Parquet.Data.Rows;

namespace Parquet.CLI.Commands
{
   abstract class FileInputCommand
   {
      private readonly string _path;

      public FileInputCommand(string path)
      {
         _path = path;
      }

      protected Table ReadTable()
      {
         using (var message = new ProgressMessage($"reading {Path.GetFileName(_path)}"))
         {
            try
            {
               using (var reader = ParquetReader.OpenFromFile(_path, new ParquetOptions { TreatByteArrayAsString = true }))
               {
                  Table table = reader.ReadAsTable();

                  return table;
               }
            }
            catch(FileNotFoundException)
            {
               message.Fail("not found: " + _path);
               throw;
            }
         }
      }
   }
}