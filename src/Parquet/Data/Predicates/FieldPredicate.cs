namespace Parquet.Data.Predicates
{
   public abstract class FieldPredicate
   {
      public abstract bool IsMatch(Thrift.ColumnChunk columnChunk, string path);

      public abstract bool IsMatch(Field field);
   }
}
