using Npgsql;
using System.Collections.Generic;
using System.Data;
using System.Linq;

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
    public class PostgresConnectionManager : DbConnectionManager<NpgsqlConnection>
    {
        public PostgresConnectionManager() : base() { }

        public PostgresConnectionManager(PostgresConnectionString connectionString) : base(connectionString) { }

        public PostgresConnectionManager(string connectionString) : base(new PostgresConnectionString(connectionString)) { }

        TableDefinition DestTableDef { get; set; }
        Dictionary<string, TableColumn> DestinationColumns { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var TN = new ObjectNameDescriptor(tableName, ConnectionManagerType.Postgres);
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
            var quotedDestColumns = destColumnNames.Select(col => TN.QB + col + TN.QE);

            using (var writer = DbConnection.BeginBinaryImport($@"
COPY {TN.QuotatedFullName} ({string.Join(", ", quotedDestColumns)})
FROM STDIN (FORMAT BINARY)"))
            {
                while (data.Read())
                {
                    writer.StartRow();
                    foreach (var destCol in destColumnNames)
                    {
                        TableColumn colDef = DestinationColumns[destCol];
                        int ordinal = data.GetOrdinal(destCol);
                        object val = data.GetValue(ordinal);
                        if (val != null)
                        {
                            object convertedVal = System.Convert.ChangeType(data.GetValue(ordinal), colDef.NETDataType);
                            writer.Write(convertedVal, colDef.InternalDataType.ToLower());
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }
                writer.Complete();
            }
        }

        public override void BeforeBulkInsert(string tableName)
        {
            DestTableDef = TableDefinition.FromName(this, tableName);
            DestinationColumns = new Dictionary<string, TableColumn>();
            foreach (var colDef in DestTableDef.Columns)
            {
                DestinationColumns.Add(colDef.Name, colDef);
            }
        }

        public override void AfterBulkInsert(string tableName)
        {
        }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            PostgresConnectionManager clone = new PostgresConnectionManager((PostgresConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
