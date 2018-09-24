using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Extensions
{
   internal static class StringBuilderExtensions
   {
      private static int NL = 2;

      public static void OpenBrace(this StringBuilder sb, int level)
      {
         if(level == -1)
         {
            sb.Append("{");
         }
         else
         {
            sb.Append(new string(' ', level * NL));
            sb.AppendLine("{");
         }
      }

      public static void CloseBrace(this StringBuilder sb, int level)
      {
         if (level == -1)
         {
            sb.Append("}");
         }
         else
         {
            sb.Append(new string(' ', level * NL));
            sb.AppendLine("}");
         }
      }
   }
}
