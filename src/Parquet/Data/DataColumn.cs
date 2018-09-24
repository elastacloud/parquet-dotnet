using System;
using System.Buffers;

namespace Parquet.Data
{
   /// <summary>
   /// The primary low-level structure to hold data for a parqut column.
   /// Handles internal data composition/decomposition to enrich with custom data Parquet format requires.
   /// </summary>
   public class DataColumn
   {
      private readonly IDataTypeHandler _dataTypeHandler;

      private DataColumn(DataField field)
      {
         Field = field ?? throw new ArgumentNullException(nameof(field));

         _dataTypeHandler = DataTypeFactory.Match(field.DataType);
      }

      /// <summary>
      /// 
      /// </summary>
      /// <param name="field"></param>
      /// <param name="data"></param>
      /// <param name="repetitionLevels"></param>
      public DataColumn(DataField field, Array data, int[] repetitionLevels = null) : this(field)
      {
         Data = data ?? throw new ArgumentNullException(nameof(data));

         RepetitionLevels = repetitionLevels;
      }

      internal DataColumn(DataField field,
         Array definedData,
         int[] definitionLevels, int maxDefinitionLevel,
         int[] repetitionLevels, int maxRepetitionLevel,
         Array dictionary,
         int[] dictionaryIndexes) : this(field)
      {
         Data = definedData;

         // 1. Dictionary merge
         if (dictionary != null)
         {
            Data = _dataTypeHandler.MergeDictionary(dictionary, dictionaryIndexes);
         }

         // 2. Apply definitions
         if (definitionLevels != null)
         {
            Data = _dataTypeHandler.UnpackDefinitions(Data, definitionLevels, maxDefinitionLevel);
         }

         // 3. Apply repetitions
         RepetitionLevels = repetitionLevels;
      }

      /// <summary>
      /// Column data where definition levels are already applied
      /// </summary>
      public Array Data { get; private set; }

      /// <summary>
      /// Repetition levels if any.
      /// </summary>
      public int[] RepetitionLevels { get; private set; }

      /// <summary>
      /// Data field
      /// </summary>
      public DataField Field { get; private set; }

      /// <summary>
      /// When true, this field has repetitions. It doesn't mean that it's an array though. This property simply checks that
      /// repetition levels are present on this column.
      /// </summary>
      public bool HasRepetitions => RepetitionLevels != null;

      internal Array PackDefinitions(int maxDefinitionLevel, out int[] pooledDefinitionLevels, out int definitionLevelCount)
      {
         pooledDefinitionLevels = ArrayPool<int>.Shared.Rent(Data.Length);
         definitionLevelCount = Data.Length;

         bool isNullable = Field.ClrType.IsNullable() || Data.GetType().GetElementType().IsNullable();

         if (!Field.HasNulls || !isNullable)
         {
            SetPooledDefinitionLevels(maxDefinitionLevel, pooledDefinitionLevels);
            return Data;
         }

         return _dataTypeHandler.PackDefinitions(Data, maxDefinitionLevel, out pooledDefinitionLevels, out definitionLevelCount);
      }

      void SetPooledDefinitionLevels(int maxDefinitionLevel, int[] pooledDefinitionLevels)
      {
         for (int i = 0; i < Data.Length; i++)
         {
            pooledDefinitionLevels[i] = maxDefinitionLevel;
         }
      }

      /// <summary>
      /// Creates a new column by combining data from both
      /// </summary>
      internal static DataColumn Merge(DataColumn column1, DataColumn column2,
         int[] rl1 = null, int[] rl2 = null)
      {
         //new data
         Array data = Array.CreateInstance(column1.Field.ClrNullableIfHasNullsType, column1.Data.Length + column2.Data.Length);
         Array.Copy(column1.Data, data, column1.Data.Length);
         Array.Copy(column2.Data, 0, data, column1.Data.Length, column2.Data.Length);

         //new RLs
         int[] rl = new int[rl1.Length + rl2.Length];
         Array.Copy(rl1, rl, rl1.Length);
         Array.Copy(rl2, 0, rl, rl1.Length, rl2.Length);

         return new DataColumn(column1.Field, data, rl);
      }
   }
}
