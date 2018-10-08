using System;
using System.IO;

namespace Parquet.CLI.Commands
{
   class ConvertCommand : FileInputCommand
   {
      private readonly string _path;

      public ConvertCommand(string path) : base(path)
      {
         _path = path;
      }

      public void Execute()
      {
         string sourceExtension = Path.GetExtension(_path);

         if(sourceExtension != ".parquet")
         {
            throw new ArgumentException($"Don't know how to read {sourceExtension}");
         }
      }
   }
}