using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.File
{
   /// <summary>
   /// Responsible for parsing file metadata from Thrift structures
   /// </summary>
   class FileMetadataParser
   {
      private readonly Thrift.FileMetaData _fileMeta;

      public FileMetadataParser(Thrift.FileMetaData fileMeta)
      {
         _fileMeta = fileMeta ?? throw new ArgumentNullException(nameof(fileMeta));
      }

      public Schema ParseSchema(ParquetOptions formatOptions)
      {
         void Build(SchemaElement node, ref int i, int count, bool isRoot)
         {
            while (node.Children.Count < count)
            {
               Thrift.SchemaElement tse = _fileMeta.Schema[i];
               SchemaElement mse;

               if (tse.Converted_type == Thrift.ConvertedType.LIST)
               {
                  Thrift.SchemaElement tseTop = tse;
                  Thrift.SchemaElement tseList = _fileMeta.Schema[++i];
                  Thrift.SchemaElement tseElement = _fileMeta.Schema[++i];

                  var tseMerged = new Thrift.SchemaElement(tse.Name);
                  tseMerged.Converted_type = tseElement.Converted_type;
                  tseMerged.Num_children = tseElement.Num_children;
                  tseMerged.Precision = tseElement.Precision;
                  tseMerged.Repetition_type = tseList.Repetition_type;
                  tseMerged.Scale = tseElement.Scale;
                  tseMerged.Type = tseElement.Type;
                  tseMerged.Type_length = tseElement.Type_length;

                  mse = new SchemaElement(tseMerged,
                     isRoot ? null : node,
                     formatOptions,
                     tseElement.Num_children == 0
                     ? typeof(IEnumerable)   //augmented to generic IEnumerable in constructor
                     : typeof(IEnumerable<Row>));
                  mse.Path = string.Join(Schema.PathSeparator, tseTop.Name, tseList.Name, tseElement.Name);
                  if (!isRoot) mse.Path = node.Path + Schema.PathSeparator + mse.Path;

                  tse = tseElement;
               }
               else
               {

                  Type containerType = tse.Num_children > 0
                     ? typeof(Row)
                     : null;

                  SchemaElement parent = isRoot ? null : node;
                  mse = new SchemaElement(tse, parent, formatOptions, containerType);
               }

               node.Children.Add(mse);

               i += 1;

               if (tse.Num_children > 0)
               {
                  Build(mse, ref i, tse.Num_children, false);
               }
            }
         }

         //extract schema tree
         var root = new SchemaElement<int>("root");
         int start = 1;
         Build(root, ref start, _fileMeta.Schema[0].Num_children, true);

         return new Schema(root.Children);
      }
   }
}
