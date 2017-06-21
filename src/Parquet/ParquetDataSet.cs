﻿/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using System.Collections.Generic;
using System.Linq;

namespace Parquet
{
   /// <summary>
   /// Represents data within parquet file
   /// </summary>
   public class ParquetDataSet
   {
      private readonly Dictionary<string, ParquetColumn> _columns = new Dictionary<string, ParquetColumn>();

      public ParquetDataSet()
      {

      }

      public ParquetDataSet(IEnumerable<ParquetColumn> columns)
      {
         foreach(ParquetColumn column in columns)
         {
            _columns.Add(column.Name, column);
         }
      }

      public ParquetDataSet(params ParquetColumn[] columns) : this((IEnumerable<ParquetColumn>)columns)
      {

      }

      /// <summary>
      /// Gets dataset columns
      /// </summary>
      public List<ParquetColumn> Columns => new List<ParquetColumn>(_columns.Values);

      /// <summary>
      /// Gets column by name
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      public ParquetColumn this[string name] => _columns[name];

      /// <summary>
      /// Merges data into this dataset
      /// </summary>
      /// <param name="source"></param>
      public void Merge(ParquetDataSet source)
      {
         if (source == null) return;

         foreach(var kv in source._columns)
         {
            if(!_columns.TryGetValue(kv.Key, out ParquetColumn col))
            {
               _columns[kv.Key] = kv.Value;

               //todo: zero values
            }
            else
            {
               col.Add(kv.Value);
            }
         }
      }

      /// <summary>
      /// Returns total number of rows
      /// </summary>
      public long Count => _columns.FirstOrDefault().Value.Values.Count;
   }

   /*public class ParquetRow
   {
      public ParquetRow(IEnumerable<object> row)
      {
                  
      }
      public Get()
      {
         foreach (var kv in source._columns)
         {
         }
   }*/
}
