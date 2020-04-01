using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox
{
    /// <summary>
    /// Defines data table
    /// </summary>
    public class TableDefinition
    {
        #region Init

        // Keep this constructor to allow new() type constraint for this type (for .NET.Core configuration system etc.)
        public TableDefinition() :
            this(null)
        { }

        public TableDefinition(string name, List<TableColumn> columns = null)
        {
            Name = name;
            Columns = columns ?? new List<TableColumn>();
        }

        public static TableDefinition FromName(IConnectionManager connectionManager, string name)
        {
            Validate(connectionManager);
            var result = new TableDefinition(name);
            GetColumns(connectionManager, name, result.Columns);
            return result;
        }

        public static implicit operator TableDefinition(string name) => new TableDefinition(name);

        #endregion

        #region Name

        public bool HasName => IsValidName(Name);
        public string Name { get; set; }

        public static bool IsValidName(string name) => !string.IsNullOrWhiteSpace(name);
        public static void ValidateName(string name, string argumentName = null)
        {
            if (IsValidName(name))
                return;
            if (argumentName is null)
                argumentName = nameof(name);
            if (name is null)
                throw new ArgumentNullException(argumentName);
            else
                throw new ArgumentException("Value cannot be white space", argumentName);
        }

        public void ValidateName(string ownerName = null) => ValidateName(
            Name,
            ownerName is null ?
                nameof(Name) :
                $"{ownerName}.{nameof(Name)}"
            );

        #endregion

        #region Columns

        public bool HasColumns => Columns.Count > 0;
        public List<TableColumn> Columns { get; }
        public int? IDColumnIndex
        {
            get
            {
                TableColumn idCol = Columns.FirstOrDefault(col => col.IsIdentity);
                if (idCol != null)
                    return Columns.IndexOf(idCol);
                else
                    return null;
            }
        }
        public string AllColumnsWithoutIdentity => Columns.Where(col => !col.IsIdentity).AsString();

        public bool EnsureColumns(IConnectionManager connectionManager)
        {
            Validate(connectionManager);
            if (HasColumns)
                return false;
            GetColumns(connectionManager, Name, Columns);
            return true;
        }

        private static void GetColumns(IConnectionManager connectionManager, string name, List<TableColumn> columns)
        {
            ValidateName(name);
            IfTableOrViewExistsTask.ThrowExceptionIfNotExists(connectionManager, name);
            ConnectionManagerType connectionType = ConnectionManagerSpecifics.GetType(connectionManager);
            ObjectNameDescriptor TN = new ObjectNameDescriptor(name, connectionType);

            if (connectionType == ConnectionManagerType.SqlServer)
                GetColumnsFromSqlServer(connectionManager, TN, columns);
            else if (connectionType == ConnectionManagerType.SQLite)
                GetColumnsFromSQLite(connectionManager, TN, columns);
            else if (connectionType == ConnectionManagerType.MySql)
                GetColumnsFromMySqlServer(connectionManager, TN, columns);
            else if (connectionType == ConnectionManagerType.Postgres)
                GetColumnsFromPostgres(connectionManager, TN, columns);
            else if (connectionType == ConnectionManagerType.Access)
                GetColumnsFromAccess(connectionManager, TN, columns);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid TableDefinition!");
        }

        private static void GetColumnsFromSqlServer(IConnectionManager connectionManager, ObjectNameDescriptor TN, List<TableColumn> columns)
        {
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
$@"
SELECT  cols.name
     , UPPER(tpes.name) AS type_name
     , cols.is_nullable
     , cols.is_identity
     , ident.seed_value
     , ident.increment_value
     , CONVERT (BIT, CASE WHEN pkidxcols.index_column_id IS NOT NULL THEN 1 ELSE 0 END ) AS primary_key
     , defconstr.definition AS default_value
     , cols.collation_name
     , compCol.definition AS computed_column_definition
FROM sys.columns cols
INNER JOIN (
    SELECT name, type, object_id, schema_id FROM sys.tables 
    UNION 
    SELECT  name, type, object_id, schema_id FROM sys.views
    ) tbl
    ON cols.object_id = tbl.object_id
INNER JOIN sys.schemas sc
    ON tbl.schema_id = sc.schema_id
INNER JOIN sys.systypes tpes
    ON tpes.xtype = cols.system_type_id
LEFT JOIN sys.identity_columns ident
    ON ident.object_id = cols.object_id
LEFT JOIN sys.indexes pkidx
    ON pkidx.object_id = cols.object_id
    AND pkidx.is_primary_key = 1
LEFT JOIN sys.index_columns pkidxcols
    on pkidxcols.object_id = cols.object_id
    AND pkidxcols.column_id = cols.column_id
    AND pkidxcols.index_id = pkidx.index_id
LEFT JOIN sys.default_constraints defconstr
    ON defconstr.parent_object_id = cols.object_id
    AND defconstr.parent_column_id = cols.column_id
LEFT JOIN sys.computed_columns compCol
    ON compCol.object_id = cols.object_id
WHERE ( CONCAt (sc.name,'.',tbl.name) ='{TN.UnquotatedFullName}' OR  tbl.name = '{TN.UnquotatedFullName}' )
    AND tbl.type IN ('U','V')
    AND tpes.name <> 'sysname'
ORDER BY cols.column_id
"
            , () => { curCol = new TableColumn(); }
            , () => { columns.Add(curCol); }
            , name => curCol.Name = name.ToString()
            , type_name => curCol.DataType = type_name.ToString()
            , is_nullable => curCol.AllowNulls = (bool)is_nullable
            , is_identity => curCol.IsIdentity = (bool)is_identity
            , seed_value => curCol.IdentitySeed = (int?)(Convert.ToInt32(seed_value))
            , increment_value => curCol.IdentityIncrement = (int?)(Convert.ToInt32(increment_value))
            , primary_key => curCol.IsPrimaryKey = (bool)primary_key
            , default_value =>
                    curCol.DefaultValue = default_value?.ToString().Substring(2, (default_value.ToString().Length) - 4)
            , collation_name => curCol.Collation = collation_name?.ToString()
            , computed_column_definition => curCol.ComputedColumn = computed_column_definition?.ToString().Substring(1, (computed_column_definition.ToString().Length) - 2)
             )
            {
                DisableLogging = true,
                ConnectionManager = connectionManager
            };
            readMetaSql.ExecuteReader();
        }

        private static void GetColumnsFromSQLite(IConnectionManager connectionManager, ObjectNameDescriptor TN, List<TableColumn> columns)
        {
            TableColumn curCol = null;
            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
        $@"PRAGMA table_info(""{TN.UnquotatedFullName}"")"
            , () => { curCol = new TableColumn(); }
            , () => { columns.Add(curCol); }
            , cid => {; }
            , name => curCol.Name = name.ToString()
            , type => curCol.DataType = type.ToString()
            , notnull => curCol.AllowNulls = (long)notnull == 1 ? true : false
            , dftl_value => curCol.DefaultValue = dftl_value?.ToString()
            , pk => curCol.IsPrimaryKey = (long)pk >= 1 ? true : false
             )
            {
                DisableLogging = true,
                ConnectionManager = connectionManager
            };
            readMetaSql.ExecuteReader();
        }

        private static void GetColumnsFromMySqlServer(IConnectionManager connectionManager, ObjectNameDescriptor TN, List<TableColumn> columns)
        {
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
$@" 
SELECT cols.column_name
  , cols.data_type
  , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS 'is_nullable'
  , CASE WHEN cols.extra IS NOT NULL AND cols.extra = 'auto_increment' THEN 1 ELSE 0 END AS 'auto_increment'
  , CASE WHEN isnull(k.constraint_name) THEN 0 ELSE 1 END AS 'primary_key'
  , cols.column_default
  , cols.collation_name
  , cols.generation_expression
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.TABLES tbl
    ON cols.table_name = tbl.table_name
    AND cols.table_schema = tbl.table_schema
    AND cols.table_catalog = tbl.table_catalog
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON cols.table_name = k.table_name
    AND cols.table_schema = k.table_schema
    AND cols.table_catalog = k.table_catalog
    AND cols.column_name = k.column_name
    AND k.constraint_name = 'PRIMARY'
WHERE ( cols.table_name = '{TN.UnquotatedFullName}'  OR  CONCAT(cols.table_catalog,'.',cols.table_name) = '{TN.UnquotatedFullName}')
    AND cols.table_schema = DATABASE()
ORDER BY cols.ordinal_position
"
            , () => { curCol = new TableColumn(); }
            , () => { columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , auto_increment => curCol.IsIdentity = (int)auto_increment == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = column_default?.ToString()
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connectionManager
            };
            readMetaSql.ExecuteReader();
        }

        private static void GetColumnsFromPostgres(IConnectionManager connectionManager, ObjectNameDescriptor TN, List<TableColumn> columns)
        {
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
$@" 
SELECT cols.column_name
,CASE 
   WHEN LEFT(cols.data_type,4) = 'time' THEN REPLACE(REPLACE(REPLACE(cols.data_type,'without time zone',''), 'with time zone', 'tz'),' ','')
   ELSE cols.data_type
END AS ""internaldatatype""
,CASE
    WHEN cols.domain_name IS NOT NULL THEN domain_name
    WHEN cols.data_type='character varying' THEN
        CASE WHEN character_maximum_length IS NULL
        THEN 'varchar'
        ELSE 'varchar('||character_maximum_length||')'
        END
    WHEN cols.data_type='character' THEN 'char('||character_maximum_length||')'
    WHEN cols.data_type='numeric' THEN
        CASE WHEN numeric_precision IS NULL
        THEN 'numeric'
        ELSE 'numeric('||numeric_precision||','||numeric_scale||')'
        END
    WHEN LEFT(cols.data_type,4) = 'time' THEN REPLACE(REPLACE(REPLACE(cols.data_type,'without time zone',''), 'with time zone', 'tz'),' ','')
    ELSE cols.data_type
END AS ""datatype""
, CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS ""is_nullable""
, CASE WHEN cols.column_default IS NOT NULL AND substring(cols.column_default,0,8) = 'nextval' THEN 1 ELSE 0 END AS ""serial""
, CASE WHEN tccu.column_name IS NULL THEN 0 ELSE 1 END AS ""primary_key""
, cols.column_default
, cols.collation_name
, cols.generation_expression
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.TABLES tbl
    ON cols.table_name = tbl.table_name
    AND cols.table_schema = tbl.table_schema
    AND cols.table_catalog = tbl.table_catalog
LEFT JOIN INFORMATION_SCHEMA.table_constraints tc
    ON cols.table_name = tc.table_name
    AND cols.table_schema = tc.table_schema
    AND cols.table_catalog = tc.table_catalog
    AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN information_schema.constraint_column_usage tccu
    ON cols.table_name = tccu.table_name
    AND cols.table_schema = tccu.table_schema
    AND cols.table_catalog = tccu.table_catalog
    AND tccu.constraint_name = tc.constraint_name
    AND tccu.constraint_schema = tc.constraint_schema
    AND tccu.constraint_catalog = tc.constraint_catalog
    AND cols.column_name = tccu.column_name
WHERE(cols.table_name = '{TN.UnquotatedFullName}'  OR  CONCAT(cols.table_schema, '.', cols.table_name) = '{TN.UnquotatedFullName}')
    AND cols.table_catalog = CURRENT_DATABASE()
ORDER BY cols.ordinal_position
"
            , () => { curCol = new TableColumn(); }
            , () => { columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , internal_type_name => curCol.InternalDataType = internal_type_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , serial => curCol.IsIdentity = (int)serial == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = column_default?.ToString().ReplaceIgnoreCase("::character varying", "")
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connectionManager
            };
            readMetaSql.ExecuteReader();
        }

        private static void GetColumnsFromAccess(IConnectionManager connectionManager, ObjectNameDescriptor TN, List<TableColumn> columns)
        {
            var accessConnection = connectionManager as AccessOdbcConnectionManager;
            accessConnection?.ReadTableDefinition(TN, columns);
        }

        #endregion

        public void CreateTable(IConnectionManager connectionManager = null)
        {
            if (connectionManager is null)
                CreateTableTask.Create(this);
            else
                CreateTableTask.Create(connectionManager, this);
        }

        private static void Validate(IConnectionManager connectionManager)
        {
            if (connectionManager is null)
                throw new ArgumentNullException(nameof(connectionManager));
        }
    }
}
