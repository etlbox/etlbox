using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace ALE.ETLBox.ConnectionManager
{
    public abstract class DbConnectionManager<Connection> : IDisposable, IConnectionManager
        where Connection : class, IDbConnection, new()
    {
        public abstract ConnectionManagerType ConnectionManagerType { get; }
        
        public int MaxLoginAttempts { get; set; } = 3;
        public bool LeaveOpen
        {
            get => _leaveOpen || IsInBulkInsert || Transaction != null;
            set => _leaveOpen = value;
        }

        public IDbConnectionString ConnectionString { get; set; }

        internal Connection DbConnection { get; set; }
        public ConnectionState? State => DbConnection?.State;
        public IDbTransaction Transaction { get; set; }
        public bool IsInBulkInsert { get; set; }
        private bool _leaveOpen;
        
        public abstract string QB { get; }
        public abstract string QE { get; }
        
        public virtual bool SupportDatabases { get; } = false;
        public virtual bool SupportDatabaseSingleUserMode { get; } = false;
        public virtual bool SupportProcedures { get; } = false;
        public virtual bool SupportProcedureCreateOrReplace { get; } = false;
        public virtual bool SupportProcedureAlter { get; } = false;
        public virtual bool SupportSchemas { get; } = false;
        public virtual bool SupportTableTruncate { get; } = false;
        public virtual bool SupportSchemaCleanUp { get; } = false;
        public virtual bool SupportComputedColumns { get; } = false;

        public DbConnectionManager()
        {
        }

        public DbConnectionManager(IDbConnectionString connectionString) : this()
        {
            this.ConnectionString = connectionString;
        }

        public void Open()
        {
            if (LeaveOpen)
            {
                if (DbConnection == null)
                {
                    DbConnection = new Connection
                    {
                        ConnectionString = ConnectionString.Value
                    };
                }
            }
            else
            {
                DbConnection?.Close();
                DbConnection = new Connection
                {
                    ConnectionString = ConnectionString.Value
                };
            }
            if (DbConnection.State != ConnectionState.Open)
            {
                TryOpenConnectionXTimes();
            }
        }

        private void TryOpenConnectionXTimes()
        {
            bool successfullyConnected = false;
            Exception lastException = null;
            for (int i = 1; i <= MaxLoginAttempts; i++)
            {
                try
                {
                    DbConnection.Open();
                    successfullyConnected = true;
                }
                catch (Exception e)
                {
                    successfullyConnected = false;
                    lastException = e;
                    Task.Delay(1000).Wait();
                }
                if (successfullyConnected)
                {
                    break;
                }
            }
            if (!successfullyConnected)
            {
                throw lastException ?? new Exception("Could not connect to database!");
            }
        }

        public IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = commandText;
            if (parameterList != null)
            {
                foreach (QueryParameter par in parameterList)
                {
                    var newPar = cmd.CreateParameter();
                    newPar.ParameterName = par.Name;
                    newPar.DbType = par.DBType;
                    newPar.Value = par.Value;
                    cmd.Parameters.Add(newPar);
                }
            }
            if (Transaction?.Connection != null && Transaction.Connection.State == ConnectionState.Open)
                cmd.Transaction = Transaction;
            return cmd;
        }

        public int ExecuteNonQuery(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteNonQuery();
        }

        public object ExecuteScalar(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteScalar();
        }

        public IDataReader ExecuteReader(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteReader();

        }

        public void BeginTransaction(IsolationLevel isolationLevel)
        {
            Open();
            Transaction = DbConnection?.BeginTransaction(isolationLevel);
        }

        public IConnectionManager CloneIfAllowed()
        {
            if (LeaveOpen) return this;
            else return Clone();
        }

        public void BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

        public void CommitTransaction()
        {
            Transaction?.Commit();
            CloseTransaction();
        }

        public void RollbackTransaction()
        {
            Transaction?.Rollback();
            CloseTransaction();
        }

        public void CloseTransaction()
        {
            Transaction.Dispose();
            Transaction = null;
            CloseIfAllowed();
        }

        public abstract void PrepareBulkInsert(string tableName);
        public abstract void CleanUpBulkInsert(string tableName);

        public abstract void BulkInsert(ITableData data, string tableName);
        public abstract void BeforeBulkInsert(string tableName);
        public abstract void AfterBulkInsert(string tableName);

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls


        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Transaction?.Dispose();
                    Transaction = null;
                    DbConnection?.Close();
                    DbConnection = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void CloseIfAllowed()
        {
            if (!LeaveOpen)
                Dispose();
        }

        public void Close()
        {
            Dispose();
        }

        public abstract IConnectionManager Clone();
        #endregion

    }
}
