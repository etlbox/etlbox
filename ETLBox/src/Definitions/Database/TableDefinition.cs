using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox
{
    public class TableDefinition
    {
        public string Name { get; set; }
        public List<TableColumn> Columns { get; set; }
        public string PrimaryKeyConstraintName { get; set; }

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


        public TableDefinition()
        {
            Columns = new List<TableColumn>();
        }

        public TableDefinition(string name) : this()
        {
            Name = name;
        }

        public TableDefinition(string name, List<TableColumn> columns) : this(name)
        {
            Columns = columns;
        }

        public void CreateTable() => CreateTableTask.Create(this);

        public void CreateTable(IConnectionManager connectionManager) => CreateTableTask.Create(connectionManager, this);

        public static TableDefinition GetDefinitionFromTableName(IConnectionManager connection, string tableName)
        {
            IfTableOrViewExistsTask.ThrowExceptionIfNotExists(connection, tableName);
            ConnectionManagerType connectionType = connection.ConnectionManagerType;
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);

            if (connectionType == ConnectionManagerType.SqlServer)
                return ReadTableDefinitionFromSqlServer(connection, TN);
            else if (connectionType == ConnectionManagerType.SQLite)
                return ReadTableDefinitionFromSQLite(connection, TN);
            else if (connectionType == ConnectionManagerType.MySql)
                return ReadTableDefinitionFromMySqlServer(connection, TN);
            else if (connectionType == ConnectionManagerType.Postgres)
                return ReadTableDefinitionFromPostgres(connection, TN);
            else if (connectionType == ConnectionManagerType.Access)
                return ReadTableDefinitionFromAccess(connection, TN);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid TableDefinition!");
        }

        private static TableDefinition ReadTableDefinitionFromSqlServer(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
$@"
SELECT  cols.name
     , CASE WHEN tpes.name IN ('varchar','char','binary','varbinary') THEN CONCAT(UPPER(tpes.name), '(', cols.max_length, ')')
            WHEN tpes.name IN ('nvarchar','nchar') THEN CONCAT(UPPER(tpes.name), '(', cols.max_length/2, ')')
            WHEN tpes.name IN ('decimal','numeric') THEN CONCAT(UPPER(tpes.name), '(', cols.precision,',',cols.scale, ')')
            ELSE UPPER(tpes.name)            
       END AS type_name
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
            , () => { result.Columns.Add(curCol); }
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
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromSQLite(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
        $@"PRAGMA table_info(""{TN.UnquotatedFullName}"")"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , cid => {; }
            , name => curCol.Name = name.ToString()
            , type => curCol.DataType = type.ToString()
            , notnull => curCol.AllowNulls = (long)notnull == 1 ? true : false
            , dftl_value => curCol.DefaultValue = dftl_value?.ToString()
            , pk => curCol.IsPrimaryKey = (long)pk >= 1 ? true : false
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromMySqlServer(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {TN.ObjectName}",
$@" 
SELECT cols.column_name
  , CASE WHEN cols.data_type IN ('varchar','char') THEN CONCAT (cols.data_type,'(',cols.character_maximum_length, ')')
	     WHEN cols.data_type IN ('decimal') THEN CONCAT (cols.data_type,'(',cols.numeric_precision,',', cols.numeric_scale, ')')
		 ELSE cols.data_type
         END AS 'data_type'
  , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS 'is_nullable'
  , CASE WHEN cols.extra IS NOT NULL AND cols.extra = 'auto_increment' THEN 1 ELSE 0 END AS 'auto_increment'
  , CASE WHEN isnull(k.constraint_name) THEN 0 ELSE 1 END AS 'primary_key'
  , cols.column_default
  , cols.collation_name
  , cols.generation_expression
  , cols.column_comment
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
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , auto_increment => curCol.IsIdentity = (int)auto_increment == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = column_default?.ToString()
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , description => curCol.Description = description?.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromPostgres(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
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
            , () => { result.Columns.Add(curCol); }
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
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromAccess(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            var accessConnection = connection as AccessOdbcConnectionManager;
            return accessConnection?.ReadTableDefinition(TN);
        }
    }
}
