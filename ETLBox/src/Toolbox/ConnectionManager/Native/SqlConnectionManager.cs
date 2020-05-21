using System.Data;
using Microsoft.Data.SqlClient;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager of a classic ADO.NET connection to a (Microsoft) Sql Server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;"));
    /// </code>
    /// </example>
    public class SqlConnectionManager : DbConnectionManager<SqlConnection>
    {
        public override ConnectionManagerType ConnectionManagerType { get; } = ConnectionManagerType.SqlServer;
        public override string QB { get; } = @"[";
        public override string QE { get; } = @"]";
        public override bool SupportDatabases { get; } = true;
        public override bool SupportDatabaseSingleUserMode { get; } = true;
        public override bool SupportProcedures { get; } = true;
        public override bool SupportProcedureAlter { get; } = true;
        public override bool SupportSchemas { get; } = true;
        public override bool SupportSchemaCleanUp { get; } = true;
        public override bool SupportTableTruncate { get; } = true;
        public override bool SupportComputedColumns { get; } = true;
        
        public bool ModifyDBSettings { get; set; } = false;

        public SqlConnectionManager() : base() { }

        public SqlConnectionManager(SqlConnectionString connectionString) : base(connectionString) { }

        public SqlConnectionManager(string connectionString) : base(new SqlConnectionString(connectionString)) { }

        string PageVerify { get; set; }
        string RecoveryModel { get; set; }
        public override void BulkInsert(ITableData data, string tableName)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, Transaction as SqlTransaction))
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in data.ColumnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override void PrepareBulkInsert(string tablename)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = this.DbConnection.Database;
                    PageVerify = this.ExecuteScalar($"SELECT page_verify_option_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    RecoveryModel = this.ExecuteScalar($"SELECT recovery_model_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    this.ExecuteNonQuery($@"USE master");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY NONE;");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY BULK_LOGGED");
                    this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch
                {
                    ModifyDBSettings = false;
                }
            }
        }

        public override void BeforeBulkInsert(string tableName) { }

        public override void AfterBulkInsert(string tableName) { }

        public override void CleanUpBulkInsert(string tablename)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = this.DbConnection.Database;
                    this.ExecuteNonQuery($@"USE master");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY {PageVerify};");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY {RecoveryModel}");
                    this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch { }
            }
        }

        public override IConnectionManager Clone()
        {
            SqlConnectionManager clone = new SqlConnectionManager((SqlConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts,
                ModifyDBSettings = this.ModifyDBSettings
            };
            return clone;
        }
    }
}
