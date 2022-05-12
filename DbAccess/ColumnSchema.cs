namespace DbAccess
{
    /// <summary>
    /// Contains the schema of a single DB column.
    /// </summary>
    public class ColumnSchema
    {
        public string ColumnName { get; set; }

        public string ColumnType { get; set; }

        public int Length { get; set; }
        public bool IsNullable { get; set; }

        public string DefaultValue { get; set; }

        public bool IsIdentity { get; set; }

        public bool? IsCaseSensitive = null;
    }
}
