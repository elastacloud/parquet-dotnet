# Reading Data

You can read the data by constructing an instance of [ParquetReader class](../src/Parquet/ParquetReader.cs) or using one of the helper classes. In the simplest case, to read a file located in `c:\data\input.parquet` you would write the following code:

```csharp
using System.IO;
using Parquet;
using Parquet.Data;

using(Stream fs = File.OpenRead("c:\\data\\input.parquet"))
{
	using(var reader = new ParquetReader(fs))
	{
		DataSet ds = reader.Read();
	}
}
```

[DataSet](../src/Parquet/Data/DataSet.cs) is a rich structure representing the data from the input file.

You can also do the same thing in one line with a helper method

```csharp
DataSet ds = ParquetReader.ReadFile("c:\\data\\input.parquet")
```

Another helper method is for reading from stream:

```csharp
using(Stream fs = File.OpenRead("c:\\data\\input.parquet"))
{
	DataSet ds = ParquetReader.Read(fs);
}
```

## Reading parts of file

todo

## Using format options

todo