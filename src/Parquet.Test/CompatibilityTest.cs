using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using NetBox;
using NetBox.FileFormats;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class PyArrowCompatibilityTest : CompatibilityTest
   {
      public PyArrowCompatibilityTest() : base("3rdparty/pyarrow/runner.py")
      {
      }
   }

   public abstract class CompatibilityTest
   {
      private readonly string _runnerRootPath;

      protected CompatibilityTest(string runnerRootPath)
      {
         _runnerRootPath = runnerRootPath;
      }

      private DataSet Run(DataSet input)
      {
         //set up input parameters
         string runnerPath = GetRunnerPath();
         string runnerRoot = new FileInfo(runnerPath).Directory.FullName;
         string inPath = Path.Combine(runnerRoot, "in.csv");
         string outPath = Path.Combine(runnerRoot, "out.parquet");

         //transform input to CSV
         ToCsv(inPath, input);

         Execute(runnerPath, inPath, outPath);

         return null;
      }

      private void ToCsv(string fileName, DataSet ds)
      {
         using (Stream fs = System.IO.File.Create(fileName))
         {
            var writer = new CsvWriter(fs);

            //write header
            IEnumerable<string> headerNames = ds.Schema.Elements.Select(se => se.Name);
            writer.Write(headerNames);

            //write values
            foreach(Row row in ds)
            {
               IEnumerable<string> values = row.RawValues.Select(v => v == null ? string.Empty : v.ToString());
               writer.Write(values);
            }
         }
      }

      private string GetRunnerPath()
      {
         //get root repository path
         string rootPath = typeof(CompatibilityTest).GetAssembly().Location;
         rootPath = Path.Combine(rootPath, "../../../../../..");
         rootPath = Path.GetFullPath(rootPath);

         //get absolute runner path
         string runnerPath = Path.Combine(rootPath, _runnerRootPath);
         runnerPath = Path.GetFullPath(runnerPath);

         return runnerPath;
      }

      private void Execute(string runnerPath, string inPath, string outPath)
      {
         var p = new Process
         {
            StartInfo = new ProcessStartInfo
            {
               //FileName = @"cmd",
               //Arguments = $"/c python.exe {runnerPath} out {inPath} {outPath}",
               FileName = "python",
               //Arguments = "-V",
               UseShellExecute = false,
               RedirectStandardError = true,
               RedirectStandardOutput = true
            }
         };

         p.Start();
         p.WaitForExit();

         string so = p.StandardOutput.ReadToEnd();
         string se = p.StandardError.ReadToEnd();

         if (!string.IsNullOrEmpty(se)) Assert.True(false, $"failed to execute: [{se}], standard output: [{so}]");
      }

      [Fact]
      public void Random_dataset_writes_and_reads()
      {
         var ds = new DataSet(new SchemaElement<int>("id"), new SchemaElement<string>("city"));

         for(int i = 0; i < Generator.GetRandomInt(1, 100); i++)
         {
            ds.Add(Generator.RandomInt, Generator.RandomString);
         }

         Run(ds);
      }
   }
}
