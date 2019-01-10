﻿using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;

namespace Parquet
{
   static class OtherExtensions
   {
      private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

      public static int GetBitWidth(this int value)
      {
         for (int i = 0; i < 64; i++)
         {
            if (value == 0) return i;
            value >>= 1;
         }

         return 1;
      }

      public static DateTime FromUnixTime(this int unixTime)
      {
         return UnixEpoch.AddDays(unixTime - 1);
      }

      public static DateTime FromUnixTime(this long unixTime)
      {
         return UnixEpoch.AddSeconds(unixTime);
      }

      public static long ToUnixTime(this DateTimeOffset date)
      {
         return Convert.ToInt64((date - UnixEpoch).TotalSeconds);
      }

      public static double ToUnixDays(this DateTimeOffset dto)
      {
         TimeSpan diff = dto - UnixEpoch;
         return diff.TotalDays;
      }

      public static long GetUnixUnixTimeDays(this DateTime date)
      {
         return Convert.ToInt64((date - UnixEpoch).TotalDays);
      }

      public static string AddPath(this string s, params string[] parts)
      {
         var path = new List<string>(parts.Length + 1);

         if (s != null) path.Add(s);
         if (parts != null) path.AddRange(parts.Where(p => p != null));

         return string.Join(Schema.PathSeparator, path);
      }

      public static bool EqualTo(this Array left, Array right)
      {
         if (left.Length != right.Length)
            return false;

         for(int i = 0; i < left.Length; i++)
         {
            object il = left.GetValue(i);
            object ir = right.GetValue(i);

            if(il == null || ir == null)
            {
               return il == null && ir == null;
            }

            if (!il.Equals(ir))
               return false;
         }

         return true;
      }

      public static Exception NotImplemented(string reason)
      {
         return new NotImplementedException($"{reason} is not yet implemented, and we are fully aware of it. From here you can either raise an issue on GitHub, implemented it, and raise a PR, or contact parquetsupport@elastacloud.com for commercial support.");
      }
   }
}
