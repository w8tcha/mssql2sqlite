namespace DbAccess
{
    using System.Collections.Generic;

    public class IndexSchema
    {
        public string IndexName { get; set; }

        public bool IsUnique { get; set; }

        public List<IndexColumn> Columns { get; set; }
    }

    public class IndexColumn
    {
        public string ColumnName { get; set; }
        public bool IsAscending { get; set; }
    }
}
