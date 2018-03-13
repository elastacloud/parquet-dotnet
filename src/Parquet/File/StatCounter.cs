namespace Parquet.File
{
   using System.Collections;

   
   class StatCounter
   {
      readonly IList _list;

      public StatCounter(IList list)
      {
         _list = list;

         //todo: calculate stats
      }
   }
}
