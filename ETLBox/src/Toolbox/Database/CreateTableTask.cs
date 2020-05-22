using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates a table. If the tables exists, this task won't change the table.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateTableTask.Create("demo.table1", new List&lt;TableColumn&gt;() {
    /// new TableColumn(name:"key", dataType:"int", allowNulls:false, isPrimaryKey:true, isIdentity:true),
    ///     new TableColumn(name:"value", dataType:"nvarchar(100)", allowNulls:true)
    /// });
    /// </code>
    /// </example>
    public class CreateTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Create table {TableName}";
        
        public void Execute()
        {
            CheckTableDefinition();
            
            bool tableExists = new IfTableOrViewExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (tableExists && ThrowErrorIfTableExists) throw new ETLBoxException($"Table {TableName} already exists!");
            if (!tableExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public void Create() => Execute();
        public TableDefinition TableDefinition { get; set; }
        public string TableName => TableDefinition.Name;
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);
        public List<TableColumn> Columns => TableDefinition.Columns;

        public bool ThrowErrorIfTableExists { get; set; }

        public string Sql
        {
            get
            {
                return
$@"CREATE TABLE {TN.QuotatedFullName} (
{ColumnsDefinitionSql}
{PrimaryKeySql}
)
";
            }
        }

        public CreateTableTask()
        {
        }
        
        public CreateTableTask(string tableName, List<TableColumn> columns) : this()
        {
            TableDefinition = new TableDefinition(tableName, columns);
        }

        public CreateTableTask(TableDefinition tableDefinition) : this()
        {
            TableDefinition = tableDefinition;
        }

        public static void Create(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).Execute();
        public static void Create(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).Execute();
        public static void Create(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.Execute();

        string ColumnsDefinitionSql
            => String.Join("  , " + Environment.NewLine, Columns?.Select(col => CreateTableDefinition(col)));

        private void CheckTableDefinition()
        {
            if (string.IsNullOrEmpty(TableName))
                throw new ETLBoxException("No table name was provided - can not create or alter the table.");
            if (Columns == null || Columns.Count == 0)
                throw new ETLBoxException("You did not provide any columns for the table - please define at least one table column.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.Name)))
                throw new ETLBoxException("One of the provided columns is either null or empty - can't create table.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.DataType)))
                throw new ETLBoxException("One of the provided columns has a datatype that is either null or empty - can't create table.");
        }

        string PrimaryKeySql => CreatePrimaryKeyConstraint();
        string CreateTableDefinition(ITableColumn col)
        {
            string dataType = string.Empty;
            dataType = CreateDataTypeSql(col);
            string identitySql = CreateIdentitySql(col);
            string collationSql = !String.IsNullOrWhiteSpace(col.Collation)
                ? $"COLLATE {col.Collation}"
                : string.Empty;
            string nullSql = CreateNotNullSql(col);
            string defaultSql = CreateDefaultSql(col);
            string computedColumnSql = CreateComputedColumnSql(col);
            string descriptionSql = CreateDescriptionSql(col);
            
            return $@"{QB}{col.Name}{QE} {dataType} {collationSql} {defaultSql} {identitySql} {nullSql} {computedColumnSql} {descriptionSql}";
        }

        private string CreateDataTypeSql(ITableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.SqlServer && col.HasComputedColumn)
                return string.Empty;
            else if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity)
                return string.Empty;
            else
                return DataTypeConverter.TryGetDBSpecificType(col.DataType, this.ConnectionType);
        }

        private string CreateIdentitySql(ITableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.SQLite) return string.Empty;
            else
            {
                if (col.IsIdentity)
                {
                    if (ConnectionType == ConnectionManagerType.MySql)
                        return "AUTO_INCREMENT";
                    else if (ConnectionType == ConnectionManagerType.Postgres)
                        return "SERIAL";
                    return $"IDENTITY({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})";
                }
                else
                {
                    return string.Empty;
                }
            }
        }

        private string CreateNotNullSql(ITableColumn col)
        {
            string nullSql = string.Empty;
            if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity) return string.Empty;
            if (ConnectionType == ConnectionManagerType.Access) return string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                nullSql = col.AllowNulls
                            ? "NULL"
                            : "NOT NULL";
            return nullSql;
        }

        private string CreatePrimaryKeyConstraint()
        {
            string result = string.Empty;
            if (Columns?.Any(col => col.IsPrimaryKey) ?? false)
            {
                var pkCols = Columns.Where(col => col.IsPrimaryKey);
                string pkConstName = TableDefinition.PrimaryKeyConstraintName ??
                        $"pk_{TN.UnquotatedFullName}_{string.Join("_", pkCols.Select(col => col.Name))}";
                string constraint = $"CONSTRAINT {QB}{pkConstName}{QE}";
                if (ConnectionType == ConnectionManagerType.SQLite) constraint = "";
                string pkConst = $", {constraint} PRIMARY KEY ({string.Join(",", pkCols.Select(col => $"{QB}{col.Name}{QE}"))})";
                return pkConst;
            }
            return result;
        }

        private string CreateDefaultSql(ITableColumn col)
        {
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)
                defaultSql = col.DefaultValue != null ? $" DEFAULT {SetQuotesIfString(col.DefaultValue)}" : string.Empty;
            return defaultSql;
        }

        private string CreateComputedColumnSql(ITableColumn col)
        {
            if (col.HasComputedColumn && !DbConnectionManager.SupportComputedColumns)
                throw new ETLBoxNotSupportedException("Computed columns are not supported.");
            
            if (col.HasComputedColumn)
                return $"AS {col.ComputedColumn}";
            else
                return string.Empty;
        }
        
        private string CreateDescriptionSql(ITableColumn col)
        {
            if (!string.IsNullOrWhiteSpace(col.Description) && DbConnectionManager.SupportDescription)
                return $"COMMENT '{col.Description}'";
            else
                return string.Empty;
        }

        string SetQuotesIfString(string value)
        {
            if (!Regex.IsMatch(value, @"^\d+(\.\d+|)$"))//@" ^ (\d|\.)+$"))
                return $"'{value}'";
            else
                return value;

        }
    }
}
