using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Represents a table or table chunk that stores data in row format.
   /// </summary>
   public class Table : IList<Row>
   {
      //dev: for reference from previous stable version see https://github.com/elastacloud/parquet-dotnet/tree/final-v2/src/Parquet/Data       

      private readonly List<Row> _rows = new List<Row>();

      /// <summary>
      /// Creates an empty table with specified schema
      /// </summary>
      /// <param name="schema"></param>
      public Table(Schema schema)
      {
         Schema = schema ?? throw new ArgumentNullException(nameof(schema));
      }

      /// <summary>
      /// Table schema
      /// </summary>
      public Schema Schema { get; }

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
            RowMatrix.Validate(value);
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
      /// 
      /// </summary>
      /// <param name="item"></param>
      public void Add(Row item)
      {
         RowMatrix.Validate(item);

         _rows.Add(item);
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
         RowMatrix.Validate(item);

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

      #endregion
   }
}