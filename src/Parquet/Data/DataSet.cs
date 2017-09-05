﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Parquet.File;

namespace Parquet.Data
{
   /// <summary>
   /// Represents dataset
   /// </summary>
   public class DataSet : IList<Row>
   {
      private readonly Schema _schema;
      private readonly List<Row> _rows = new List<Row>();
      private readonly DataSetMetadata _metadata = new DataSetMetadata();

      /// <summary>
      /// Gets dataset schema
      /// </summary>
      public Schema Schema => _schema;

      /// <summary>
      /// Gets the public metadata
      /// </summary>
      public DataSetMetadata Metadata => _metadata;

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(Schema schema)
      {
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(params SchemaElement[] schema)
      {
         if (schema == null) throw new ArgumentNullException(nameof(schema));
         if (schema.Length == 0) throw new ArgumentException("schema must not be empty", nameof(schema));

         _schema = new Schema(schema);
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="DataSet"/> class.
      /// </summary>
      /// <param name="schema">The schema.</param>
      public DataSet(IEnumerable<SchemaElement> schema)
      {
         if (schema == null) throw new ArgumentNullException(nameof(schema));

         _schema = new Schema(schema);

         if(_schema.Length == 0) throw new ArgumentException("schema must not be empty", nameof(schema));
      }


      /// <summary>
      /// Slices rows and returns list of all values in a particular column.
      /// </summary>
      /// <param name="i">Column index</param>
      /// <param name="offset">The offset.</param>
      /// <param name="count">The count.</param>
      /// <returns>
      /// Column values
      /// </returns>
      public IList GetColumn(int i, int offset = 0, int count = -1)
      {
         SchemaElement schema = Schema.Elements[i];
         IList result = TypeFactory.Create(schema.ElementType, schema.IsNullable);

         for(int irow = offset; (count == -1 || result.Count < count) && (irow < _rows.Count); irow++)
         {
            Row row = _rows[irow];
            result.Add(row[i]);
         }

         return result;
      }

      /// <summary>
      /// Slices rows and returns list of all values in a particular column.
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="offset">The offset.</param>
      /// <param name="count">The count.</param>
      /// <returns>
      /// Column values
      /// </returns>
      /// <exception cref="ArgumentException"></exception>
      public IList GetColumn(string name, int offset = 0, int count = -1)
      {
         for(int i = 0; i < _schema.Elements.Count; i++)
         {
            if (_schema.Elements[i].Name == name) return GetColumn(i, offset, count);
         }

         throw new ArgumentException($"cannot find column {name}");
      }

      /// <summary>
      /// Adds the specified values.
      /// </summary>
      /// <param name="values">The values.</param>
      public void Add(params object[] values)
      {
         Add(new Row(values));
      }

      /// <summary>
      /// Used to merge and add columns to a dataset 
      /// </summary>
      /// <param name="ds">A second dataset to merge this one with</param>
      public DataSet Merge(DataSet ds)
      {
         // Get the new schema that we have to build fold the row copies into a new view
         List<SchemaElement> additionalSchema = CoalesceRows(ds);
         // Create a new dataset form the previous two
         var newSchema = new List<SchemaElement>();
         newSchema.AddRange(Schema.Elements);
         newSchema.AddRange(additionalSchema);
         var mergedDs = new DataSet(newSchema);

         // now that we've got the datasets let's build the rows 
         int biggestRows = this.RowCount < ds.RowCount ? ds.RowCount : this.RowCount;
         int totalColumns = this.ColumnCount + additionalSchema.Count;
         // build in the first addition of nulls from dataset 0
         // TODO: Fold all of this up and add to comparer and design a union operator 
         for (int i = 0; i < this.RowCount; i++)
         {
            List<object> newValues = new List<object>();
            newValues.AddRange(this[i].RawValues);
            newValues.AddRange(new object[additionalSchema.Count]);
            mergedDs.Add(newValues.ToArray());
         }
         // Build the second part of the dataset but nulling out the initial except for any coincidence in values
         // TODO: At the moment only works for the singular case of non-coalescing rows
         for (int i = 0; i < ds.RowCount; i++)
         {
            List<object> newValues = new List<object>();
            newValues.AddRange(new object[ColumnCount]);
            newValues.AddRange(ds[i].RawValues);
            mergedDs.Add(newValues.ToArray());
         }
         return mergedDs;
      }
      
      private List<SchemaElement> CoalesceRows(DataSet ds)
      {
         // get the columns!!
         var combinedRowNames = new List<SchemaElement>();
         var additionalRowNames = new List<SchemaElement>();
         // go through the available columns in the second dataset
         foreach (SchemaElement schemaElement in ds._schema.Elements)
         {
            foreach (SchemaElement schemaElementThis in _schema.Elements)
            {
               if (schemaElementThis.Name == schemaElement.Name &&
                   schemaElementThis.ElementType == schemaElement.ElementType)
               {
                  // then this element belongs to this schema so we can add but need to define the superset
                  // this can then be added to the existing rowset
                  combinedRowNames.Add(schemaElement);
               }
               else if (schemaElementThis.Name == schemaElement.Name &&
                        schemaElementThis.ElementType == schemaElement.ElementType)
               {
                  throw new ParquetException(
                     $"unable to merge schema, {schemaElement.Name} has different types in schemas");
               }
               else
               {
                  // Add the new column but null out the details if the column isn't nullable then throw an exception
                  if (!schemaElement.IsNullable)
                  {
                     throw new ParquetException($"unable to merge schema, {schemaElement.Name} is not nullable");
                  }
                  // add new null column value here
                  additionalRowNames.Add(schemaElement);
               }
            }
         }
         return additionalRowNames;
      }

      private void Validate(Row row)
      {
         if (row == null)
            throw new ArgumentNullException(nameof(row));

         int rl = row.Length;

         if (rl != _schema.Length)
            throw new ArgumentException($"the row has {rl} values but schema expects {_schema.Length}", nameof(row));

         for(int i = 0; i < rl; i++)
         {
            object rowValue = row[i];
            SchemaElement se = _schema.Elements[i];
            Type elementType = se.ElementType;

            if (rowValue == null)
            {
               se.IsNullable = true;
            }
            else
            {
               if (rowValue.GetType() != elementType)
                  throw new ArgumentException($"column '{se.Name}' expects '{elementType}' but {rowValue.GetType()} passed");
            }
         }
      }

      #region [ IList members ]

      /// <summary>
      /// Gets row by index
      /// </summary>
      public Row this[int index] { get => _rows[index]; set => _rows[index] = value; }

      /// <summary>
      /// Gets the number of rows contained in this dataset.
      /// </summary>
      public int RowCount => _rows.Count;

      /// <summary>
      /// Gets the total row count in the source file this dataset was read from
      /// </summary>
      public long TotalRowCount { get; internal set; }

      /// <summary>
      /// Gets the number of columns contained in this dataset
      /// </summary>
      public int Count => _rows.Count;

      /// <summary>
      /// Gets the number of columns contained in this dataset
      /// </summary>
      public int ColumnCount => Schema.Elements.Count;

      /// <summary>
      /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
      /// </summary>
      public bool IsReadOnly => false;

      /// <summary>
      /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </summary>
      /// <param name="row">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
      public void Add(Row row)
      {
         Validate(row);

         _rows.Add(row);
      }

      /// <summary>
      /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </summary>
      public void Clear()
      {
         _rows.Clear();
      }

      /// <summary>
      /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1" /> contains a specific value.
      /// </summary>
      /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
      /// <returns>
      /// true if <paramref name="item" /> is found in the <see cref="T:System.Collections.Generic.ICollection`1" />; otherwise, false.
      /// </returns>
      public bool Contains(Row item)
      {
         return _rows.Contains(item);
      }

      /// <summary>
      /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1" /> to an <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
      /// </summary>
      /// <param name="array">The one-dimensional <see cref="T:System.Array" /> that is the destination of the elements copied from <see cref="T:System.Collections.Generic.ICollection`1" />. The <see cref="T:System.Array" /> must have zero-based indexing.</param>
      /// <param name="arrayIndex">The zero-based index in <paramref name="array" /> at which copying begins.</param>
      public void CopyTo(Row[] array, int arrayIndex)
      {
         _rows.CopyTo(array, arrayIndex);
      }

      /// <summary>
      /// Returns an enumerator that iterates through the collection.
      /// </summary>
      /// <returns>
      /// An enumerator that can be used to iterate through the collection.
      /// </returns>
      public IEnumerator<Row> GetEnumerator()
      {
         return _rows.GetEnumerator();
      }

      /// <summary>
      /// Determines the index of a specific item in the <see cref="T:System.Collections.Generic.IList`1" />.
      /// </summary>
      /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.IList`1" />.</param>
      /// <returns>
      /// The index of <paramref name="item" /> if found in the list; otherwise, -1.
      /// </returns>
      public int IndexOf(Row item)
      {
         return _rows.IndexOf(item);
      }

      /// <summary>
      /// Inserts an item to the <see cref="T:System.Collections.Generic.IList`1" /> at the specified index.
      /// </summary>
      /// <param name="index">The zero-based index at which <paramref name="row" /> should be inserted.</param>
      /// <param name="row">The object to insert into the <see cref="T:System.Collections.Generic.IList`1" />.</param>
      public void Insert(int index, Row row)
      {
         Validate(row);

         _rows.Insert(index, row);
      }

      /// <summary>
      /// Removes the first occurrence of a specific object from the <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </summary>
      /// <param name="item">The object to remove from the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
      /// <returns>
      /// true if <paramref name="item" /> was successfully removed from the <see cref="T:System.Collections.Generic.ICollection`1" />; otherwise, false. This method also returns false if <paramref name="item" /> is not found in the original <see cref="T:System.Collections.Generic.ICollection`1" />.
      /// </returns>
      public bool Remove(Row item)
      {
         return _rows.Remove(item);
      }

      /// <summary>
      /// Removes the <see cref="T:System.Collections.Generic.IList`1" /> item at the specified index.
      /// </summary>
      /// <param name="index">The zero-based index of the item to remove.</param>
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
