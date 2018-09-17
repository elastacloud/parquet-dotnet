﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Represents a table or table chunk that stores data in row format.
   /// </summary>
   public class Table : IList<Row>, IEquatable<Table>
   {
      //dev: for reference from previous stable version see https://github.com/elastacloud/parquet-dotnet/tree/final-v2/src/Parquet/Data       

      private readonly List<Row> _rows = new List<Row>();
      private readonly Field[] _dfs;

      /// <summary>
      /// Creates an empty table with specified schema
      /// </summary>
      /// <param name="schema">Parquet file schema.</param>
      public Table(Schema schema)
      {
         Schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _dfs = schema.Fields.ToArray();
      }

      /// <summary>
      /// Creates a table with specified schema
      /// </summary>
      /// <param name="schema">Parquet file schema.</param>
      /// <param name="tableData">Optionally initialise this table with data columns that correspond to the passed <paramref name="schema"/></param>
      /// <param name="rowCount"></param>
      internal Table(Schema schema, DataColumn[] tableData, long rowCount) : this(schema)
      {
         Schema = schema ?? throw new ArgumentNullException(nameof(schema));
         _dfs = schema.Fields.ToArray();

         if(tableData != null)
         {
            _rows.AddRange(RowMatrix.Compact(schema, tableData, rowCount));
         }
      }

      /// <summary>
      /// Table schema
      /// </summary>
      public Schema Schema { get; }

      internal DataColumn[] ExtractDataColumns()
      {
         return RowMatrix.Extract(Schema, _rows);
      }

      #region [ IList members ]

      /// <summary>
      /// 
      /// </summary>
      /// <param name="index"></param>
      /// <returns></returns>
      public Row this[int index]
      {
         get => _rows[index];
         set
         {
            RowMatrix.Validate(value, _dfs);
            _rows[index] = value;
         }
      }

      /// <summary>
      /// 
      /// </summary>
      public int Count => _rows.Count;

      /// <summary>
      /// 
      /// </summary>
      public bool IsReadOnly => false;

      /// <summary>
      /// Adds a new row
      /// </summary>
      /// <param name="item"></param>
      public void Add(Row item)
      {
         RowMatrix.Validate(item, _dfs);

         _rows.Add(item);
      }

      /// <summary>
      /// Adds a new row from passed cells
      /// </summary>
      /// <param name="rowCells"></param>
      public void Add(params object[] rowCells)
      {
         var row = new Row(rowCells);

         Add(row);
      }

      /// <summary>
      /// /
      /// </summary>
      public void Clear()
      {
         _rows.Clear();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      public bool Contains(Row item)
      {
         return _rows.Contains(item);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="array"></param>
      /// <param name="arrayIndex"></param>
      public void CopyTo(Row[] array, int arrayIndex)
      {
         _rows.CopyTo(array, arrayIndex);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <returns></returns>
      public IEnumerator<Row> GetEnumerator()
      {
         return _rows.GetEnumerator();
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      public int IndexOf(Row item)
      {
         return _rows.IndexOf(item);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="index"></param>
      /// <param name="item"></param>
      public void Insert(int index, Row item)
      {
         RowMatrix.Validate(item, _dfs);

         _rows.Insert(index, item);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="item"></param>
      /// <returns></returns>
      public bool Remove(Row item)
      {
         return _rows.Remove(item);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="index"></param>
      public void RemoveAt(int index)
      {
         _rows.RemoveAt(index);
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _rows.GetEnumerator();
      }

      /// <summary>
      /// Compares tables for equality, including:
      /// - schema equality
      /// - row count
      /// - row values equality
      /// </summary>
      /// <param name="other"></param>
      /// <returns></returns>
      public bool Equals(Table other)
      {
         return Equals(other, false);
      }

      /// <summary>
      /// Compares tables for equality, including:
      /// - schema equality
      /// - row count
      /// - row values equality
      /// </summary>
      /// <param name="other"></param>
      /// <param name="throwExceptions"></param>
      /// <returns></returns>
      public bool Equals(Table other, bool throwExceptions)
      {
         if (ReferenceEquals(other, null))
         {
            if (throwExceptions)
               throw new ArgumentNullException(nameof(other));

            return false;
         }

         if (!other.Schema.Equals(Schema))
         {
            if(throwExceptions)
               throw new ArgumentException(Schema.GetNotEqualsMessage(other.Schema, "this", "other"));

            return false;
         }

         if (other.Count != Count)
         {
            if (throwExceptions)
               throw new ArgumentException($"expected {Count} rows but found {other.Count}");

            return false;
         }

         for(int i = 0; i < Count; i++)
         {
            if (!this[i].Equals(other[i]))
            {
               if (throwExceptions)
                  throw new ArgumentException($"rows are different at row {i}. this: {this[i]}, other: {other[i]}");

               return false;
            }
         }

         return true;
      }

      #endregion
   }
}