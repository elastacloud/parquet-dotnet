﻿namespace Parquet
{
   using File;
   using System;
   using System.Collections.Generic;
   using System.IO;
   using Data;
   using System.Collections;
   using Data.Predicates;
   using System.Linq;
   using Thrift;


   /// <summary>
   /// Implements Apache Parquet format reader
   /// </summary>
   public class ParquetReader :
      ParquetActor,
      IDisposable
   {
      readonly Stream _input;
      FileMetaData _meta;
      readonly ParquetOptions _formatOptions;
      readonly ReaderOptions _readerOptions;
      readonly FieldPredicate[] _fieldPredicates;

      /// <summary>
      /// Creates an instance from input stream
      /// </summary>
      /// <param name="input">Input stream, must be readable and seekable</param>
      /// <param name="formatOptions">Optional reader options</param>
      /// <param name="readerOptions">The reader options.</param>
      /// <exception cref="ArgumentNullException">input</exception>
      /// <exception cref="ArgumentException">stream must be readable and seekable - input</exception>
      /// <exception cref="IOException">not a Parquet file (size too small)</exception>
      public ParquetReader(Stream input, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null) : base(input)
      {
         _input = input ?? throw new ArgumentNullException(nameof(input));
         if (!input.CanRead || !input.CanSeek) throw new ArgumentException("stream must be readable and seekable", nameof(input));
         if (_input.Length <= 8) throw new IOException("not a Parquet file (size too small)");

         ValidateFile();
         _formatOptions = formatOptions ?? new ParquetOptions();
         _readerOptions = readerOptions ?? new ReaderOptions();
         _fieldPredicates = PredicateFactory.CreateFieldPredicates(_readerOptions);
      }

      /// <summary>
      /// Reads the file
      /// </summary>
      /// <param name="fullPath">The full path.</param>
      /// <param name="formatOptions">Optional reader options.</param>
      /// <param name="readerOptions">The reader options.</param>
      /// <returns><see cref="DataSet"/></returns>
      public static DataSet ReadFile(string fullPath, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null)
      {
         using (Stream fs = System.IO.File.OpenRead(fullPath))
         {
            using (var reader = new ParquetReader(fs, formatOptions, readerOptions))
            {
               return reader.Read();
            }
         }
      }

      /// <summary>
      /// Reads <see cref="DataSet"/> from an open stream
      /// </summary>
      /// <param name="source">Input stream</param>
      /// <param name="formatOptions">Parquet options, optional.</param>
      /// <param name="readerOptions">Reader options, optional</param>
      /// <returns><see cref="DataSet"/></returns>
      public static DataSet Read(Stream source, ParquetOptions formatOptions = null, ReaderOptions readerOptions = null)
      {
         using (var reader = new ParquetReader(source, formatOptions, readerOptions))
         {
            return reader.Read();
         }
      }

      /// <summary>
      /// Test read, to be defined
      /// </summary>
      public DataSet Read()
      {
         _readerOptions.Validate();

         _meta = ReadMetadata();

         var footer = new ThriftFooter(_meta);

         var pathToValues = new Dictionary<string, IList>();
         long pos = 0;
         long rowsRead = 0;

         foreach(Thrift.RowGroup rg in _meta.Row_groups)
         {
            //check whether to skip RG completely
            if ((_readerOptions.Count != -1 && rowsRead >= _readerOptions.Count) ||
               (_readerOptions.Offset > pos + rg.Num_rows - 1))
            {
               pos += rg.Num_rows;
               continue;
            }

            long offset = Math.Max(0, _readerOptions.Offset - pos);
            long count = _readerOptions.Count == -1 ? rg.Num_rows : Math.Min(_readerOptions.Count - rowsRead, rg.Num_rows);

            for(int icol = 0; icol < rg.Columns.Count; icol++)
            {
               Thrift.ColumnChunk cc = rg.Columns[icol];
               string path = cc.GetPath();
               if (_fieldPredicates != null && !_fieldPredicates.Any(p => p.IsMatch(cc, path))) continue;

               var columnarReader = new ColumnarReader(_input, cc, footer, _formatOptions);

               try
               {
                  IList chunkValues = columnarReader.Read(offset, count);

                  if(!pathToValues.TryGetValue(path, out IList allValues))
                  {
                     pathToValues[path] = chunkValues;
                  }
                  else
                  {
                     foreach(object v in chunkValues)
                     {
                        allValues.Add(v);
                     }
                  }

                  if(icol == 0)
                  {
                     //todo: this may not work
                     rowsRead += chunkValues.Count;
                  }
               }
               catch(Exception ex)
               {
                  throw new ParquetException($"fatal error reading column '{path}'", ex);
               }
            }

            pos += rg.Num_rows;
         }

         Schema schema = footer.CreateModelSchema(_formatOptions);
         schema = schema.Filter(_fieldPredicates);
         var ds = new DataSet(schema, pathToValues, _meta.Num_rows, _meta.Created_by);
         Dictionary<string, string> customMetadata = footer.CustomMetadata;
         if (customMetadata != null) ds.Metadata.Custom.AddRange(customMetadata);
         ds.Thrift = _meta;
         return ds;
      }

      /// <summary>
      /// Disposes 
      /// </summary>
      public void Dispose()
      {
      }
   }
}