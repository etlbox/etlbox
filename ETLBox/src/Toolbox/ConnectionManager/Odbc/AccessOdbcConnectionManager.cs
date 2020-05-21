using System;
using System.Data;
using System.Data.Odbc;
using System.Runtime.CompilerServices;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for an ODBC connection to Access databases.
    /// This connection manager also is based on ADO.NET.
    /// ODBC by default does not support a Bulk Insert - and Access does not support the insert into (...) values (...),(...),(...)
    /// syntax. So the following syntax is used
    /// <code>
    /// insert into (Col1, Col2,...)
    /// select * from (
    ///   select 'Val1' as Col1 from dummytable
    ///   union all
    ///   select 'Val2' as Col2 from dummytable
    ///   ...
    /// ) a;
    /// </code>
    ///
    /// The dummytable is a special helper table containing only one record.
    ///
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection =
    ///   new AccessOdbcConnectionManager(new OdbcConnectionString(
    ///      "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\DB\Test.mdb"));
    /// </code>
    /// </example>
    public class AccessOdbcConnectionManager : OdbcConnectionManager
    {
        public override ConnectionManagerType ConnectionManagerType { get; } = ConnectionManagerType.Access;
        public override string QB { get; } = @"[";
        public override string QE { get; } = @"]";

        public AccessOdbcConnectionManager() : base() {
            LeaveOpen = true;
        }
        public AccessOdbcConnectionManager(OdbcConnectionString connectionString)
            : base(connectionString) {
            LeaveOpen = true;
        }
        public AccessOdbcConnectionManager(string connectionString) :
            base(new OdbcConnectionString(connectionString)) {
            LeaveOpen = true;
        }

        /// <summary>
        /// Helper table that needs to be created in order to simulate bulk inserts.
        /// Contains only 1 record and is only temporarily created.
        /// </summary>
        public string DummyTableName { get; set; } = "etlboxdummydeleteme";
        protected bool PreparationDone { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql bulkInsert = new BulkInsertSql()
            {
                ConnectionType = ConnectionManagerType.Access,
                QB = QB,
                QE = QE,
                UseParameterQuery = true,
                AccessDummyTableName = DummyTableName,
            };
            OdbcBulkInsert(data, tableName, bulkInsert);
        }

        public bool CheckIfTableOrViewExists(string unquotatedFullName)
        {
            try
            {
                DataTable schemaTables = GetSchemaDataTable(unquotatedFullName, "Tables");
                if (schemaTables.Rows.Count > 0)
                    return true;
                else {
                    DataTable schemaViews = GetSchemaDataTable(unquotatedFullName, "Views");
                    if (schemaViews.Rows.Count > 0)
                        return true;
                }
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private DataTable GetSchemaDataTable(string unquotatedFullName, string schemaInfo)
        {
            this.Open();
            string[] restrictions = new string[3];
            restrictions[2] = unquotatedFullName;
            DataTable schemaTable = DbConnection.GetSchema(schemaInfo, restrictions);
            return schemaTable;

        }

        internal TableDefinition ReadTableDefinition(ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            DataTable schemaTable = GetSchemaDataTable(TN.UnquotatedFullName, "Columns");

            foreach (var row in schemaTable.Rows)
            {
                DataRow dr = row as DataRow;
                TableColumn col = new TableColumn()
                {
                    Name = dr[schemaTable.Columns["COLUMN_NAME"]].ToString(),
                    DataType = dr[schemaTable.Columns["TYPE_NAME"]].ToString(),
                    AllowNulls = dr[schemaTable.Columns["IS_NULLABLE"]].ToString() == "YES" ? true : false
                };
                result.Columns.Add(col);
            }

            return result;
        }

        public override void PrepareBulkInsert(string tablename)
        {
            TryDropDummyTable();
            CreateDummyTable();
        }

        public override void CleanUpBulkInsert(string tablename) {
            TryDropDummyTable();
        }

        public override void BeforeBulkInsert(string tableName)
        {
            if (!PreparationDone)
                PrepareBulkInsert(tableName);
        }
        public override void AfterBulkInsert(string tableName)
        {
        }

        private void TryDropDummyTable()
        {
            try
            {
                ExecuteCommand($@"DROP TABLE {DummyTableName};");
            }
            catch { }
        }

        private void CreateDummyTable()
        {
            ExecuteCommand($@"CREATE TABLE {DummyTableName} (Field1 NUMBER);");
            ExecuteCommand($@"INSERT INTO { DummyTableName} VALUES(1);");
            PreparationDone = true;
        }

        private void ExecuteCommand(string sql)
        {
            if (DbConnection == null) this.Open();
            var cmd = DbConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override IConnectionManager Clone()
        {
            AccessOdbcConnectionManager clone = new AccessOdbcConnectionManager((OdbcConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
