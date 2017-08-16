using System;
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
               int childCount = tse.Num_children;
               bool isContainer = childCount > 0;
               Type containerType = isContainer
                  ? (tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED ? typeof(IEnumerable<Row>) : typeof(Row))
                  : null;

               SchemaElement parent = isRoot ? null : node;
               var mse = new SchemaElement(tse, parent, formatOptions, containerType);
               node.Children.Add(mse);

               i += 1;

               if (tse.Num_children > 0)
               {
                  Build(mse, ref i, childCount, false);
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
