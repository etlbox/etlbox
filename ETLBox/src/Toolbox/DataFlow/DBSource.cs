using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    /// <example>
    /// <code>
    /// DbSource&lt;MyRow&gt; source = new DbSource&lt;MyRow&gt;("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DbSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        #region Init

        private DbSource(IConnectionManager connectionManager = null) :
            base(connectionManager)
            => typeInfo = new DBTypeInfo(typeof(TOutput));

        public DbSource(TableDefinition tableDefinition, IConnectionManager connectionManager = null) :
            this(connectionManager)
            => TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));

        public DbSource(string sql, IConnectionManager connectionManager = null) :
            this(connectionManager)
        {
            if (sql is null)
                throw new ArgumentNullException(nameof(sql));
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Value cannot be white space", nameof(sql));
            Sql = sql;
        }

        #endregion

        /* ITask Interface */
        public override string TaskName => $"Read data from {SourceDescription}";

        /* Public Properties */
        public bool HasTableDefinition => TableDefinition != null;
        public TableDefinition TableDefinition { get; }
        public bool HasSql => !string.IsNullOrWhiteSpace(Sql);
        public string Sql { get; }
        public List<string> ColumnNames { get; set; }

        public string SqlForRead
        {
            get
            {
                if (HasSql)
                    return Sql;
                else
                {
                    TableDefinition.EnsureColumns(DbConnectionManager);
                    var TN = new ObjectNameDescriptor(TableDefinition.Name, ConnectionType);
                    return $@"SELECT {TableDefinition.Columns.AsString("", QB, QE)} FROM {TN.QuotatedFullName}";
                }

            }
        }

        public List<string> ColumnNamesEvaluated
        {
            get
            {
                if (ColumnNames?.Count > 0)
                    return ColumnNames;
                else if (HasTableDefinition)
                    return TableDefinition?.Columns?.Select(col => col.Name).ToList();
                else
                    return ParseColumnNamesFromQuery();
            }
        }

        private readonly DBTypeInfo typeInfo;
        string SourceDescription
        {
            get
            {
                if (HasTableDefinition)
                    return $"table {TableDefinition.Name}";
                else
                    return "custom SQL";
            }
        }

        private List<string> ParseColumnNamesFromQuery()
        {
            var result = SqlParser.ParseColumnNames(QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead);
            if (typeInfo.IsArray && result?.Count == 0) throw new ETLBoxException("Could not parse column names from Sql Query! Please pass a valid TableDefinition to the " +
                " property SourceTableDefinition with at least a name for each column that you want to use in the source."
                );
            return result;
        }

        public override void Execute()
        {
            NLogStart();
            ReadAll();
            Buffer.Complete();
            NLogFinish();
        }

        private void ReadAll()
        {
            SqlTask sqlT = CreateSqlTask(SqlForRead);
            DefineActions(sqlT, ColumnNamesEvaluated);
            sqlT.ExecuteReader();
            CleanupSqlTask(sqlT);
        }

        SqlTask CreateSqlTask(string sql)
        {
            var sqlT = new SqlTask(this, sql)
            {
                DisableLogging = true,
            };
            sqlT.Actions = new List<Action<object>>();
            return sqlT;
        }

        TOutput _row;
        internal void DefineActions(SqlTask sqlT, List<string> columnNames)
        {
            _row = default(TOutput);
            if (typeInfo.IsArray)
            {
                sqlT.BeforeRowReadAction = () =>
                    _row = (TOutput)Activator.CreateInstance(typeof(TOutput), new object[] { columnNames.Count });
                int index = 0;
                foreach (var colName in columnNames)
                    index = SetupArrayFillAction(sqlT, index);
            }
            else
            {
                if (columnNames?.Count == 0) columnNames = typeInfo.PropertyNames;
                foreach (var colName in columnNames)
                {
                    if (typeInfo.HasPropertyOrColumnMapping(colName))
                        SetupObjectFillAction(sqlT, colName);
                    else if (typeInfo.IsDynamic)
                        SetupDynamicObjectFillAction(sqlT, colName);
                    else
                        sqlT.Actions.Add(col => { });
                }
                sqlT.BeforeRowReadAction = () => _row = (TOutput)Activator.CreateInstance(typeof(TOutput));
            }
            sqlT.AfterRowReadAction = () =>
            {
                if (_row != null)
                {
                    LogProgress();
                    Buffer.SendAsync(_row).Wait();
                }
            };
        }

        private int SetupArrayFillAction(SqlTask sqlT, int index)
        {
            int currentIndexAvoidingClosure = index;
            sqlT.Actions.Add(col =>
            {
                try
                {
                    if (_row != null)
                    {
                        var ar = _row as System.Array;
                        var con = Convert.ChangeType(col, typeof(TOutput).GetElementType());
                        ar.SetValue(con, currentIndexAvoidingClosure);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
            index++;
            return index;
        }

        private void SetupObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        var propInfo = typeInfo.GetInfoByPropertyNameOrColumnMapping(colName);
                        var con = colValue != null ? Convert.ChangeType(colValue, typeInfo.UnderlyingPropType[propInfo]) : colValue;
                        propInfo.TrySetValue(_row, con);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        private void SetupDynamicObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        dynamic r = _row as ExpandoObject;
                        ((IDictionary<String, Object>)r).Add(colName, colValue);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        void CleanupSqlTask(SqlTask sqlT)
        {
            sqlT.Actions = null;
        }


    }

    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets. The non generic version of the DbSource uses a dynamic object that contains the data.
    /// </summary>
    /// <see cref="DbSource{TOutput}"/>
    /// <example>
    /// <code>
    /// DbSource source = new DbSource("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DbSource : DbSource<ExpandoObject>
    {
        public DbSource(TableDefinition tableDefinition, IConnectionManager connectionManager = null) :
            base(tableDefinition, connectionManager)
        { }

        public DbSource(string sql, IConnectionManager connectionManager = null) :
            base(sql, connectionManager)
        { }
    }
}
