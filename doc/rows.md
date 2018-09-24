# Row Based Access

> *v3 originally completely killed row-based access, however that also killed some use cases associated with the ease of use.*

> *Comparing to v2, v3 doesn't expand/contract rows added to the `Table` because most of the use cases with row based access is around processing data before it's written to parquet. v3 however agressively validates rows added to the tables before accepting those changes.*


## Table

Table is at the root of row-based hierarchy. A table is simply a collection of `Row`s, and by itself implements `IList<Row>` interface. 

## Row

Row is a central structure to hold data during row-based access. Essentially a row is an array of untyped objects. The fact that the row holds untyped objects *adds a performance penalty on working with rows and tables* throught parquet, because all of the data cells needs to be boxed/unboxed for reading and writing. If you can work with *column-based data* please don't use row-based access at all. However, if you absolutely need row-based access, these helper classes are still better than writing your own helper classes.

## Flat Data

todo

## Arrays (Repeatable fields)

todo

## Dictionaries (Maps)

```csharp
var schema = new Schema(
   new DataField<string>("city"),
   new MapField("population",
      new DataField<int>("areaId"),
      new DataField<long>("count")));
```

and you need to write a row that has *London* as a city and *population* is a map of *234=>100, 235=>110*.

The table should look like:

|         |Column 0|Column 1|
|---------|--------|--------|
|**Row 0**|London|`List<Row>`|

where the last cell is the data for your map. As w're in the row-based world, this needs to be represented as a list of rows as well:

|         |Column 0|Column 1|
|---------|--------|--------|
|**Row 0**|234|100|
|**Row 1**|235|110|

## Lists

todo

## Structures

todo