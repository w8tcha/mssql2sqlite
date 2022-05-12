namespace Converter
{
    using System;
    using System.Collections;
    using System.Configuration;

    public static class Configuration
  {
    public static Hashtable Section => (Hashtable) ConfigurationManager.GetSection("mssql2sqlite") ?? new Hashtable();

    public static string SqlServer
    {
      get
      {
        var sqlServer = ConfigurationManager.ConnectionStrings["SqlServer"];
        if (sqlServer == null)
        {
          throw new ConfigurationErrorsException("Please add a valid connectionstring with the key SqlServer");
        }

        return sqlServer.ConnectionString;
      }
    }

    public static string Password => GetValue((string)Section["password"], string.Empty);

    public static string Sqlite => GetValue((string)Section["sqlite"], "sqlite.db");

    public static string Tables => GetValue((string)Section["tables"], ".*");

    public static ExportMode Mode => GetEnumValue((string)Section["mode"], ExportMode.Gui);

    public static bool ExportTriggers => GetEnumValue((string)Section["exportTriggers"], false);

    public static string GetValue(string enumString, string defaultValue)
    {
      return string.IsNullOrEmpty(enumString) ? defaultValue : enumString;
    }

    public static bool GetValue(string enumString, bool defaultValue)
    {
        if (string.IsNullOrEmpty(enumString))
        {
            return defaultValue;
        }

        return bool.TryParse(enumString, out var value) ? value : defaultValue;
    }

    public static T GetEnumValue<T>(string enumString, T defaultValue)
    {
      if (string.IsNullOrEmpty(enumString) || Enum.IsDefined(typeof(T), enumString) == false)
      {
        return defaultValue;
      }

      return (T) Enum.Parse(typeof(T), enumString);
    }

    public enum ExportMode
    {
      Console,
      Gui
    }
  }
}
