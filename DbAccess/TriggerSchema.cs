/// <summary>
/// The DbAccess namespace.
/// </summary>
namespace DbAccess
{
    /// <summary>
    /// Enum TriggerEvent
    /// </summary>
    public enum TriggerEvent
    {
        /// <summary>
        /// The delete
        /// </summary>
        Delete,
        /// <summary>
        /// The update
        /// </summary>
        Update,
        /// <summary>
        /// The insert
        /// </summary>
        Insert
    }

    /// <summary>
    /// Enum TriggerType
    /// </summary>
    public enum TriggerType
    {
        /// <summary>
        /// The after
        /// </summary>
        After,
        /// <summary>
        /// The before
        /// </summary>
        Before
    }

    /// <summary>
    /// Class TriggerSchema.
    /// </summary>
    public class TriggerSchema
    {
        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }
        /// <summary>
        /// Gets or sets the event.
        /// </summary>
        /// <value>The event.</value>
        public TriggerEvent Event { get; set; }
        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        public TriggerType Type { get; set; }
        /// <summary>
        /// Gets or sets the body.
        /// </summary>
        /// <value>The body.</value>
        public string Body { get; set; }
        /// <summary>
        /// Gets or sets the table.
        /// </summary>
        /// <value>The table.</value>
        public string Table { get; set; }
    }
}