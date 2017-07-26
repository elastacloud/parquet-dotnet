﻿using System;
using System.Collections;
using System.IO;
using Parquet.Data;
using System.Linq;
using System.Collections.Generic;

namespace Parquet.File.Values
{
   class PlainDictionaryValuesWriter : IValuesWriter
   {
      public bool Write(BinaryWriter writer, SchemaElement schema, IList data)
      {
         IList dictionary;
         List<int> indexes;

         if (!GetData(data, schema, out dictionary, out indexes)) return false;


         throw new NotImplementedException();
      }

      private bool GetData(IList data, SchemaElement schema, out IList dictionary, out List<int> indexes)
      {
         dictionary = null;
         indexes = null;

         if (data.Count == 0) return true;

         if (schema.ElementType != typeof(string)) return false; //only support strings in dictionaries for now

         dictionary = data
            .Cast<string>()
            .Distinct()
            .ToList();

         float ratio = dictionary.Count / (float)data.Count;
         if (ratio > 0.75) return false;        //only compress if there are less than 75% uniqueue values

         //map uniqueue values to indexes
         var valueToIndex = new Dictionary<string, int>();
         for (int i = 0; i < dictionary.Count; i++)
         {
            valueToIndex[(string)dictionary[i]] = i;
         }

         //map values to indexes
         indexes = data
            .Cast<string>()
            .Select(v => valueToIndex[v])
            .ToList();

         return true;
      }
   }
}
