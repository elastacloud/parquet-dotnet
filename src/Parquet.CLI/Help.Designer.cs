﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Parquet.CLI {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "15.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Help {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Help() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("Parquet.CLI.Help", typeof(Help).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Path to parquet file..
        /// </summary>
        internal static string Argument_Path {
            get {
                return ResourceManager.GetString("Argument_Path", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Displays first N rows of a parquet file..
        /// </summary>
        internal static string Command_Head_Description {
            get {
                return ResourceManager.GetString("Command_Head_Description", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Output format..
        /// </summary>
        internal static string Command_Head_Format {
            get {
                return ResourceManager.GetString("Command_Head_Format", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Maximum number of rows to display. Hard limit is 100..
        /// </summary>
        internal static string Command_Head_Max {
            get {
                return ResourceManager.GetString("Command_Head_Max", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Displays parquet file schema as Parquet.Net sees it. Note that this is a simplified, human-readable version of the schema..
        /// </summary>
        internal static string Command_Schema_Description {
            get {
                return ResourceManager.GetString("Command_Schema_Description", resourceCulture);
            }
        }
    }
}
