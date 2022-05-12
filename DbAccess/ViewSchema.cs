namespace DbAccess
{
    /// <summary>
    /// Describes a single view schema
    /// </summary>
    public class ViewSchema
    {
        /// <summary>
        /// Contains the view name
        /// </summary>
        public string ViewName { get; set; }

        /// <summary>
        /// Contains the view SQL statement
        /// </summary>
        public string ViewSQL { get; set; }
    }
}
