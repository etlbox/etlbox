using System.Data.Odbc;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Sql Connection manager for an ODBC connection based on ADO.NET to Sql Server.
    /// ODBC by default does not support a Bulk Insert - inserting big amounts of data is translated into a
    /// <code>
    /// insert into (...) values (..),(..),(..) statementes.
    /// </code>
    /// This means that inserting big amounts of data in a database via Odbc can be much slower
    /// than using the native connector.
    /// Also be careful with the batch size - some databases have limitations regarding the length of sql statements.
    /// Reduce the batch size if you encounter issues here.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection =
    ///   new OdbcConnectionManager(new ObdcConnectionString(
    ///     "Driver={SQL Server};Server=.;Database=ETLBox;Trusted_Connection=Yes;"));
    /// </code>
    /// </example>
    public class SqlOdbcConnectionManager : OdbcConnectionManager
    {
        public override ConnectionManagerType ConnectionManagerType { get; } = ConnectionManagerType.SqlServer;
        public override string QB { get; } = @"[";
        public override string QE { get; } = @"]";
        public override bool SupportDatabases { get; } = true;
        public override bool SupportProcedures { get; } = true;
        public override bool SupportProcedureAlter { get; } = true;
        public override bool SupportSchemas { get; } = true;
        public override bool SupportTableTruncate { get; } = true;
        public override bool SupportComputedColumns { get; } = true;
        
        public SqlOdbcConnectionManager() : base() { }

        public SqlOdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }
        public SqlOdbcConnectionManager(string connectionString) : base(new OdbcConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql bulkInsert = new BulkInsertSql()
            {
                UseParameterQuery = true,
                QB = QB,
                QE = QE,
                //ConnectionType = ConnectionManagerType.SqlServer
            };
            OdbcBulkInsert(data, tableName, bulkInsert);
        }

        public override IConnectionManager Clone()
        {
            SqlOdbcConnectionManager clone = new SqlOdbcConnectionManager((OdbcConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }

        public override void BeforeBulkInsert(string tableName) { }
        public override void AfterBulkInsert(string tableName) { }
        public override void PrepareBulkInsert(string tablename) { }
        public override void CleanUpBulkInsert(string tablename) { }
    }
}
