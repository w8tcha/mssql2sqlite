namespace Converter
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Forms;

    using DbAccess;

    /// <summary>
    /// The dialog allows the user to select which tables to include in the 
    /// conversion process.
    /// </summary>
    public partial class TableSelectionDialog : Form
    {
        public TableSelectionDialog()
        {
            this.InitializeComponent();
        }

        /// <summary>
        /// Returns the list of included table schema objects.
        /// </summary>
        public List<TableSchema> IncludedTables =>
            (from DataGridViewRow row in this.grdTables.Rows
             let include = (bool)row.Cells[0].Value
             where include
             select (TableSchema)row.Tag).ToList();

        /// <summary>
        /// Opens the table selection dialog and uses the specified schema list in order
        /// to update the tables grid.
        /// </summary>
        /// <param name="schema">The DB schema to display in the grid</param>
        /// <param name="owner">The owner form</param>
        /// <returns>dialog result according to user decision.</returns>
        public DialogResult ShowTables(List<TableSchema> schema, IWin32Window owner)
        {
            this.UpdateGuiFromSchema(schema);
            return this.ShowDialog(owner);
        }

        private void OK_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.OK;
        }

        private void Cancel_Click(object sender, EventArgs e)
        {
            this.DialogResult = DialogResult.Cancel;
        }

        private void DeselectAll_Click(object sender, EventArgs e)
        {
            foreach (DataGridViewRow row in this.grdTables.Rows)
            {
                // Uncheck the [V] for this row.
                row.Cells[0].Value = false;
            }
        }

        private void SelectAll_Click(object sender, EventArgs e)
        {
            foreach (DataGridViewRow row in this.grdTables.Rows)
            {
                // Check the [V] for this row.
                row.Cells[0].Value = true;
            }
        }

        private void UpdateGuiFromSchema(List<TableSchema> schema)
        {
            this.grdTables.Rows.Clear();
            foreach (var table in schema)
            {
                this.grdTables.Rows.Add(true, table.TableName);
                this.grdTables.Rows[this.grdTables.Rows.Count - 1].Tag = table;
            }
        }
    }
}