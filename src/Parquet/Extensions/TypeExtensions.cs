namespace Parquet
{
   using System;
   using System.Collections.Generic;
   using System.Collections;
   using System.Reflection;

   
   static class TypeExtensions
   {
      /// <summary>
      /// Checks if this type implements generic IEnumerable or an array.
      /// </summary>
      /// <param name="type"></param>
      /// <param name="baseType"></param>
      /// <returns></returns>
      public static bool TryExtractEnumerableType(this Type type, out Type baseType)
      {
         if (typeof(byte[]) == type)
         {
            //it's a special case to avoid confustion between byte arrays and repeatable bytes
            baseType = null;
            return false;
         }

         TypeInfo typeInfo = type.GetTypeInfo();
         Type[] args = typeInfo.GenericTypeArguments;

         if (args.Length == 1)
         {
            //check derived interfaces
            foreach (Type interfaceType in typeInfo.ImplementedInterfaces)
            {
               TypeInfo interfaceTypeInfo = interfaceType.GetTypeInfo();
               if (!interfaceTypeInfo.IsGenericType || interfaceTypeInfo.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                  continue;
               
               baseType = typeInfo.GenericTypeArguments[0];
               return true;
            }

            //check if this is an IEnumerable<>
            if (typeInfo.IsGenericType && typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
               baseType = typeInfo.GenericTypeArguments[0];
               return true;
            }
         }

         if (typeInfo.IsArray)
         {
            baseType = typeInfo.GetElementType();
            return true;
         }

         baseType = null;
         return false;
      }

      public static bool TryExtractDictionaryType(this Type type, out Type keyType, out Type valueType)
      {
         TypeInfo typeInfo = type.GetTypeInfo();

         if (typeInfo.IsGenericType && typeInfo.GetGenericTypeDefinition().GetTypeInfo().IsAssignableFrom(typeof(Dictionary<,>).GetTypeInfo()))
         {
            keyType = typeInfo.GenericTypeArguments[0];
            valueType = typeInfo.GenericTypeArguments[1];
            return true;
         }

         keyType = valueType = null;
         return false;
      }

      public static bool IsNullable(this IList list)
      {
         TypeInfo typeInfo = list.GetType().GetTypeInfo();

         Type type = typeInfo.GenericTypeArguments[0];
         Type genericType = type.GetTypeInfo().IsGenericType ? type.GetTypeInfo().GetGenericTypeDefinition() : null;

         return genericType == typeof(Nullable<>) || type.GetTypeInfo().IsClass;
      }

      public static bool IsNullable(this Type t)
      {
         TypeInfo typeInfo = t.GetTypeInfo();

         return typeInfo.IsClass || (typeInfo.IsGenericType && typeInfo.GetGenericTypeDefinition() == typeof(Nullable<>));
      }

      public static Type GetNonNullable(this Type type)
      {
         TypeInfo typeInfo = type.GetTypeInfo();

         return typeInfo.IsClass ? type : typeInfo.GenericTypeArguments[0];
      }

      public static bool IsSimple(this Type t)
      {
         if (t == null) return true;

         return
            t == typeof(bool) ||
            t == typeof(byte) ||
            t == typeof(sbyte) ||
            t == typeof(char) ||
            t == typeof(decimal) ||
            t == typeof(double) ||
            t == typeof(float) ||
            t == typeof(int) ||
            t == typeof(uint) ||
            t == typeof(long) ||
            t == typeof(ulong) ||
            t == typeof(short) ||
            t == typeof(ushort) ||
            t == typeof(TimeSpan) ||
            t == typeof(DateTime) ||
            t == typeof(Guid) ||
            t == typeof(string);
      }
   }
}