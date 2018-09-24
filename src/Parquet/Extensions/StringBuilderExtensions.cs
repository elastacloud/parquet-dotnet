using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Extensions
{
   internal static class StringBuilderExtensions
   {
      private static int NL = 2;
      private const string BraceOpen = "{";
      private const string BraceClose = "}";

      public static void Ident(this StringBuilder sb, int level)
      {
         if (level == -1)
            return;

         sb.Append(new string(' ', level * NL));
      }

      public static void StartEntity(this StringBuilder sb, int level)
      {
         if (level == -1)
            return;

         sb.AppendLine();
      }

      public static void OpenBrace(this StringBuilder sb, int level, string brace = BraceOpen)
      {
         if(level == -1)
         {
            sb.Append(brace);
         }
         else
         {
            sb.Append(new string(' ', level * NL));
            sb.Append(brace);
            sb.AppendLine();
         }
      }

      public static void CloseBrace(this StringBuilder sb, int level, string brace = BraceClose)
      {
         if (level == -1)
         {
            sb.Append(brace);
         }
         else
         {
            sb.Append(new string(' ', level * NL));
            sb.Append(brace);
         }
      }
   }
}
