using System;
using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for an ODBC connection to Acccess databases.
    /// This connection manager also is based on ADO.NET.
    /// ODBC by default does not support a Bulk Insert - and Access does not supoport the insert into (...) values (...),(...),(...)
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
        public AccessOdbcConnectionManager() : base() { }
        public AccessOdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }
        public AccessOdbcConnectionManager(string connectionString) : base(new OdbcConnectionString(connectionString)) { }

        /// <summary>
        /// Helper table that needs to be created in order to simulate bulk inserts.
        /// Contains only 1 record and is only temporarily created.
        /// </summary>
        public string DummyTableName { get; set; } = "etlboxdummydeleteme";

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql bulkInsert = new BulkInsertSql()
            {
                ConnectionType = ConnectionManagerType.Access,
                AccessDummyTableName = DummyTableName,
                UseParameterQuery = true
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
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

        internal void ReadTableDefinition(ObjectNameDescriptor TN, List<TableColumn> columns)
        {
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
                columns.Add(col);
            }
        }

        public override void BeforeBulkInsert(string tableName)
        {
            TryDropDummyTable();
            CreateDummyTable();
        }
        public override void AfterBulkInsert(string tableName)
        {
            TryDropDummyTable();
        }

        private void TryDropDummyTable()
        {
            try
            {
                ExecuteCommandOnSameConnection($@"DROP TABLE {DummyTableName};");
            }
            catch { }
        }

        private void CreateDummyTable()
        {
            ExecuteCommandOnSameConnection($@"CREATE TABLE {DummyTableName} (Field1 NUMBER);");
            ExecuteCommandOnSameConnection($@"INSERT INTO { DummyTableName} VALUES(1);");
        }

        private void ExecuteCommandOnSameConnection(string sql)
        {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            AccessOdbcConnectionManager clone = new AccessOdbcConnectionManager((OdbcConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }


    }
}
