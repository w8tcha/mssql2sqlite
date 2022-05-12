namespace DbAccess
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// The trigger builder.
    /// </summary>
    public static class TriggerBuilder
    {
        public static IList<TriggerSchema> GetForeignKeyTriggers(TableSchema dt)
        {
            IList<TriggerSchema> result = new List<TriggerSchema>();

            foreach (var fks in from fks in dt.ForeignKeys let sb = new StringBuilder() select fks)
            {
                result.Add(GenerateInsertTrigger(fks));
                result.Add(GenerateUpdateTrigger(fks));
                result.Add(GenerateDeleteTrigger(fks));
            }

            return result;
        }

        private static string MakeTriggerName(ForeignKeySchema fks, string prefix)
        {
            return $"{prefix}_{fks.TableName}_{fks.ColumnName}_{fks.ForeignTableName}_{fks.ForeignColumnName}";
        }

        /// <summary>
        /// The generate insert trigger.
        /// </summary>
        /// <param name="fks">
        /// The fks.
        /// </param>
        /// <returns>
        /// The <see cref="TriggerSchema"/>.
        /// </returns>
        public static TriggerSchema GenerateInsertTrigger(ForeignKeySchema fks)
        {
            var trigger = new TriggerSchema
                              {
                                  Name = MakeTriggerName(fks, "fki"), Type = TriggerType.Before, Event = TriggerEvent.Insert,
                                  Table = fks.TableName
                              };

            var nullString = string.Empty;
            if (fks.IsNullable)
            { 
                nullString = $" NEW.{fks.ColumnName} IS NOT NULL AND";
            }
             
            trigger.Body =
                $"SELECT RAISE(ROLLBACK, 'insert on table {fks.TableName} violates foreign key constraint {trigger.Name}') WHERE{nullString} (SELECT {fks.ForeignColumnName} FROM {fks.ForeignTableName} WHERE {fks.ForeignColumnName} = NEW.{fks.ColumnName}) IS NULL; ";
            return trigger;
        }

        /// <summary>
        /// The generate update trigger.
        /// </summary>
        /// <param name="fks">
        /// The fks.
        /// </param>
        /// <returns>
        /// The <see cref="TriggerSchema"/>.
        /// </returns>
        public static TriggerSchema GenerateUpdateTrigger(ForeignKeySchema fks)
        {
            var trigger = new TriggerSchema
                              {
                                  Name = MakeTriggerName(fks, "fku"), Type = TriggerType.Before, Event = TriggerEvent.Update,
                                  Table = fks.TableName
                              };

            var triggerName = trigger.Name;
            var nullString = string.Empty;
            if (fks.IsNullable)
            {
                nullString = $" NEW.{fks.ColumnName} IS NOT NULL AND";
            }

            trigger.Body =
                $"SELECT RAISE(ROLLBACK, 'update on table {fks.TableName} violates foreign key constraint {triggerName}') WHERE{nullString} (SELECT {fks.ForeignColumnName} FROM {fks.ForeignTableName} WHERE {fks.ForeignColumnName} = NEW.{fks.ColumnName}) IS NULL; ";

            return trigger;
        }

        /// <summary>
        /// The generate delete trigger.
        /// </summary>
        /// <param name="fks">
        /// The fks.
        /// </param>
        /// <returns>
        /// The <see cref="TriggerSchema"/>.
        /// </returns>
        public static TriggerSchema GenerateDeleteTrigger(ForeignKeySchema fks)
        {
            var trigger = new TriggerSchema
                              {
                                  Name = MakeTriggerName(fks, "fkd"), Type = TriggerType.Before, Event = TriggerEvent.Delete,
                                  Table = fks.ForeignTableName
                              };

            var triggerName = trigger.Name;
            
            trigger.Body = !fks.CascadeOnDelete ? $"SELECT RAISE(ROLLBACK, 'delete on table {fks.ForeignTableName} violates foreign key constraint {triggerName}') WHERE (SELECT {fks.ColumnName} FROM {fks.TableName} WHERE {fks.ColumnName} = OLD.{fks.ForeignColumnName}) IS NOT NULL; " : $"DELETE FROM [{fks.TableName}] WHERE {fks.ColumnName} = OLD.{fks.ForeignColumnName}; ";
            return trigger;
        }
    }
}
