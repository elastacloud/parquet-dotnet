using System;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.Serialization;

namespace Parquet.Extensions
{
   internal static class StringBuilderExtensions
   {
      private const string BraceOpen = "{";
      private const string BraceClose = "}";

      public static void StartArray(this StringBuilder sb, StringFormat sf, int level)
      {
         switch (sf)
         {
            case StringFormat.Json:
               if(level == 0)
               {
                  sb.AppendLine();
               }
               else
               {
                  sb.Append("[");
               }
               break;
            default:
               sb.Append("[");
               break;
         }
      }

      public static void EndArray(this StringBuilder sb, StringFormat sf, int level)
      {
         switch (sf)
         {
            case StringFormat.Json:
               if(level > 0)
               {
                  sb.Append("]");
               }
               break;
            default:
               sb.Append("]");
               break;
         }
      }

      public static void DivideObjects(this StringBuilder sb, StringFormat sf, int level)
      {
         switch (sf)
         {
            case StringFormat.Json:
               if (level > 0)
               {
                  sb.Append(",");
               }
               else
               {
                  sb.AppendLine();
               }
               break;
            default:
               sb.Append(";");
               break;
         }
         
      }

      public static void StartObject(this StringBuilder sb, StringFormat sf)
      {
         sb.Append("{");
      }

      public static void EndObject(this StringBuilder sb, StringFormat sf)
      {
         sb.Append("}");
      }

      public static void AppendPropertyName(this StringBuilder sb, StringFormat sf, Field f)
      {
         switch (sf)
         {
            case StringFormat.Json:
               if (f != null)
               {
                  sb.Append("\"");
                  sb.Append(f?.Name ?? "?");
                  sb.Append("\": ");
               }
               break;
         }
      }

      public static void AppendNull(this StringBuilder sb, StringFormat sf)
      {
         switch (sf)
         {
            case StringFormat.Json:
               sb.Append("null");
               break;
            default:
               sb.Append("<null>");
               break;
         }
      }

      public static void Append(this StringBuilder sb, StringFormat sf, object value)
      {
         switch (sf)
         {
            case StringFormat.Json:
               EncodeJson(sb, value);
               break;
            default:
               sb.Append(value.ToString());
               break;
         }
      }

      private static void EncodeJson(StringBuilder sb, object value)
      {
         Type t = value.GetType();

         if (t == typeof(string))
         {
            sb.Append("\"");
            sb.Append(HttpEncoder.JavaScriptStringEncode((string)value));
            sb.Append("\"");
         }
         else if(t == typeof(DateTimeOffset))
         {
            sb.Append("\"");
            sb.Append(value.ToString());
            sb.Append("\"");
         }
         else
         {
            sb.Append(value.ToString());
         }

      }
   }
}
