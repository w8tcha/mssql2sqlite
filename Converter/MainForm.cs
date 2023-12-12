namespace Converter
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Reflection;
    using System.Windows.Forms;
    
    using DbAccess;

    public partial class MainForm : Form
    {
        #region Constructor

        public MainForm()
        {
            this.InitializeComponent();
        }

        #endregion

        #region Event Handler

        private void btnBrowseSQLitePath_Click(object sender, EventArgs e)
        {
            var res = this.saveFileDialog1.ShowDialog(this);
            if (res == DialogResult.Cancel)
                return;

            var fpath = this.saveFileDialog1.FileName;
            this.txtSQLitePath.Text = fpath;
            this.pbrProgress.Value = 0;
            this.lblMessage.Text = string.Empty;
        }

        private void cboDatabases_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.UpdateSensitivity();
            this.pbrProgress.Value = 0;
            this.lblMessage.Text = string.Empty;
        }

        private void btnSet_Click(object sender, EventArgs e)
        {
            try
            {
                var constr = this.cbxIntegrated.Checked
                                 ? GetSqlServerConnectionString(this.txtSqlAddress.Text, "master")
                                 : GetSqlServerConnectionString(
                                     this.txtSqlAddress.Text,
                                     "master",
                                     this.txtUserDB.Text,
                                     this.txtPassDB.Text);

                using (var conn = new SqlConnection(constr))
                {
                    conn.Open();

                    // Get the names of all DBs in the database server.
                    var query = new SqlCommand(@"select distinct [name] from sysdatabases", conn);
                    using (var reader = query.ExecuteReader())
                    {
                        this.cboDatabases.Items.Clear();
                        while (reader.Read())
                        {
                            this.cboDatabases.Items.Add((string) reader[0]);
                        }

                        if (this.cboDatabases.Items.Count > 0)
                        {
                            this.cboDatabases.SelectedIndex = 0;
                        }
                    }
                }

                this.cboDatabases.Enabled = true;

                this.pbrProgress.Value = 0;
                this.lblMessage.Text = string.Empty;
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, ex.Message, "Failed To Connect", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void txtSQLitePath_TextChanged(object sender, EventArgs e)
        {
            this.UpdateSensitivity();
        }

        private void MainForm_Load(object sender, EventArgs e)
        {
            this.UpdateSensitivity();

            var version = Assembly.GetExecutingAssembly().GetName().Version.ToString();
            this.Text = $"SQL Server To SQLite DB Converter ({version})";
        }

        private void txtSqlAddress_TextChanged(object sender, EventArgs e)
        {
            this.UpdateSensitivity();
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            SqlServerToSQLite.CancelConversion();
        }

        private void MainForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (SqlServerToSQLite.IsActive)
            {
                SqlServerToSQLite.CancelConversion();
                this._shouldExit = true;
                e.Cancel = true;
            }
            else
                e.Cancel = false;
        }

        private void cbxEncrypt_CheckedChanged(object sender, EventArgs e)
        {
            this.UpdateSensitivity();
        }

        private void txtPassword_TextChanged(object sender, EventArgs e)
        {
            this.UpdateSensitivity();
        }

        private void ChkIntegratedCheckedChanged(object sender, EventArgs e)
        {
            if (this.cbxIntegrated.Checked)
            {
                this.lblPassword.Visible = false;
                this.lblUser.Visible = false;
                this.txtPassDB.Visible = false;
                this.txtUserDB.Visible = false;
            }
            else
            {
                this.lblPassword.Visible = true;
                this.lblUser.Visible = true;
                this.txtPassDB.Visible = true;
                this.txtUserDB.Visible = true;
            }
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            var sqlConnString = this.cbxIntegrated.Checked
                                    ? GetSqlServerConnectionString(
                                        this.txtSqlAddress.Text,
                                        (string) this.cboDatabases.SelectedItem)
                                    : GetSqlServerConnectionString(
                                        this.txtSqlAddress.Text,
                                        (string) this.cboDatabases.SelectedItem,
                                        this.txtUserDB.Text,
                                        this.txtPassDB.Text);

            var createViews = this.cbxCreateViews.Checked;

            var sqlitePath = this.txtSQLitePath.Text.Trim();
            this.Cursor = Cursors.WaitCursor;
            var handler = new SqlConversionHandler(
                delegate(bool done, bool success, int percent, string msg)
                    {
                        this.Invoke(
                            new MethodInvoker(
                                delegate()
                                    {
                                        this.UpdateSensitivity();
                                        this.lblMessage.Text = msg;
                                        this.pbrProgress.Value = percent;

                                        if (!done)
                                        {
                                            return;
                                        }

                                        this.btnStart.Enabled = true;
                                        this.Cursor = Cursors.Default;
                                        this.UpdateSensitivity();

                                        if (success)
                                        {
                                            MessageBox.Show(
                                                this,
                                                msg,
                                                "Conversion Finished",
                                                MessageBoxButtons.OK,
                                                MessageBoxIcon.Information);
                                            this.pbrProgress.Value = 0;
                                            this.lblMessage.Text = string.Empty;
                                        }
                                        else
                                        {
                                            if (!this._shouldExit)
                                            {
                                                MessageBox.Show(
                                                    this,
                                                    msg,
                                                    "Conversion Failed",
                                                    MessageBoxButtons.OK,
                                                    MessageBoxIcon.Error);
                                                this.pbrProgress.Value = 0;
                                                this.lblMessage.Text = string.Empty;
                                            }
                                            else
                                            {
                                                Application.Exit();
                                            }
                                        }
                                    }));
                    });
            var selectionHandler = new SqlTableSelectionHandler(
                delegate(List<TableSchema> schema)
                    {
                        List<TableSchema> updated = null;
                        this.Invoke(
                            new MethodInvoker(
                                delegate
                                    {
                                        // Allow the user to select which tables to include by showing him the 
                                        // table selection dialog.
                                        var dlg = new TableSelectionDialog();
                                        var res = dlg.ShowTables(schema, this);
                                        if (res == DialogResult.OK)
                                        {
                                            updated = dlg.IncludedTables;
                                        }
                                    }));
                        return updated;
                    });

            var viewFailureHandler = new FailedViewDefinitionHandler(
                delegate(ViewSchema vs)
                    {
                        string updated = null;
                        this.Invoke(
                            new MethodInvoker(
                                delegate
                                    {
                                        var dlg = new ViewFailureDialog();
                                        dlg.View = vs;
                                        var res = dlg.ShowDialog(this);
                                        updated = res == DialogResult.OK ? dlg.ViewSql : null;
                                    }));

                        return updated;
                    });

            var password = this.txtPassword.Text.Trim();
            if (!this.cbxEncrypt.Checked)
            {
                password = null;
            }

            SqlServerToSQLite.ConvertSqlServerToSQLiteDatabase(
                sqlConnString,
                sqlitePath,
                password,
                handler,
                selectionHandler,
                viewFailureHandler,
                this.cbxTriggers.Checked,
                createViews);
        }

        #endregion

        #region Private Methods

        private void UpdateSensitivity()
        {
            if (this.txtSQLitePath.Text.Trim().Length > 0 && this.cboDatabases.Enabled
                                                          && (!this.cbxEncrypt.Checked
                                                              || this.txtPassword.Text.Trim().Length > 0))
            {
                this.btnStart.Enabled = true && !SqlServerToSQLite.IsActive;
            }
            else
            {
                this.btnStart.Enabled = false;
            }

            this.btnSet.Enabled = this.txtSqlAddress.Text.Trim().Length > 0 && !SqlServerToSQLite.IsActive;
            this.btnCancel.Visible = SqlServerToSQLite.IsActive;
            this.txtSqlAddress.Enabled = !SqlServerToSQLite.IsActive;
            this.txtSQLitePath.Enabled = !SqlServerToSQLite.IsActive;
            this.btnBrowseSQLitePath.Enabled = !SqlServerToSQLite.IsActive;
            this.cbxEncrypt.Enabled = !SqlServerToSQLite.IsActive;
            this.cboDatabases.Enabled = this.cboDatabases.Items.Count > 0 && !SqlServerToSQLite.IsActive;
            this.txtPassword.Enabled = this.cbxEncrypt.Checked && this.cbxEncrypt.Enabled;
            this.cbxIntegrated.Enabled = !SqlServerToSQLite.IsActive;
            this.cbxCreateViews.Enabled = !SqlServerToSQLite.IsActive;
            this.cbxTriggers.Enabled = !SqlServerToSQLite.IsActive;
            this.txtPassDB.Enabled = !SqlServerToSQLite.IsActive;
            this.txtUserDB.Enabled = !SqlServerToSQLite.IsActive;
        }

        private static string GetSqlServerConnectionString(string address, string db)
        {
            var res = $@"Data Source={address.Trim()};Initial Catalog={db.Trim()};Integrated Security=SSPI;";
            return res;
        }

        private static string GetSqlServerConnectionString(string address, string db, string user, string pass)
        {
            var res =
                $@"Data Source={address.Trim()};Initial Catalog={db.Trim()};User ID={user.Trim()};Password={pass.Trim()}";
            return res;
        }

        #endregion

        #region Private Variables

        private bool _shouldExit;

        #endregion
    }
}