namespace DbAccess
{
    using System.Collections.Generic;

    /// <summary>
    /// Contains the entire database schema
    /// </summary>
    public class DatabaseSchema
    {
        /// <summary>
        /// Gets or sets the tables.
        /// </summary>
        /// <value>The tables.</value>
        public List<TableSchema> Tables { get; set; } = new List<TableSchema>();

        /// <summary>
        /// Gets or sets the views.
        /// </summary>
        /// <value>The views.</value>
        public List<ViewSchema> Views { get; set; } = new List<ViewSchema>();
    }
}
