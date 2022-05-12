namespace DbAccess
{
    public class ForeignKeySchema
    {
        public string TableName { get; set; }

        public string ColumnName { get; set; }

        public string ForeignTableName { get; set; }

        public string ForeignColumnName { get; set; }

        public bool CascadeOnDelete { get; set; }

        public bool IsNullable { get; set; }
    }
}