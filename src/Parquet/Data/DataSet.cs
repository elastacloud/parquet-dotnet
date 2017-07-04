using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Generics;
using System.Linq;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset
   /// </summary>
   public class DataSet : IList<Row>
   {
      private Schema _schema;
      private readonly List<Row> _rows = new List<Row>();

      public Schema Schema => _schema;

      internal void AddColumnar(Schema schema, IList<IList> columnsList)
      {
         _schema = schema;

         IEnumerator[] iear = columnsList.Select(c => c.GetEnumerator()).ToArray();
         iear.ForEach(ie => ie.Reset());

         while (iear.All(ie => ie.MoveNext()))
         {
            var row = new Row(iear.Select(ie => ie.Current));
            _rows.Add(row);
         }
      }

      #region [ IList members ]

      public Row this[int index] { get => _rows[index]; set => _rows[index] = value; }

      public int Count => _rows.Count;

      public bool IsReadOnly => false;

      public void Add(Row item)
      {
         //todo: validate schema

         _rows.Add(item);
      }

      public void Clear()
      {
         _rows.Clear();
      }

      public bool Contains(Row item)
      {
         return _rows.Contains(item);
      }

      public void CopyTo(Row[] array, int arrayIndex)
      {
         _rows.CopyTo(array, arrayIndex);
      }

      public IEnumerator<Row> GetEnumerator()
      {
         return _rows.GetEnumerator();
      }

      public int IndexOf(Row item)
      {
         return _rows.IndexOf(item);
      }

      public void Insert(int index, Row item)
      {
         //todo: validate schema

         _rows.Insert(index, item);
      }

      public bool Remove(Row item)
      {
         return _rows.Remove(item);
      }

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
