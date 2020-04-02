using ALE.ETLBox.ConnectionManager;
using System;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Will create a database if the database doesn't exists. In MySql, this will create a schema.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateDatabaseTask.Create("DemoDB");
    /// </code>
    /// </example>
    public class CreateDatabaseTask : GenericTask, ITask
    {
        public CreateDatabaseTask(string databaseName = null, string collation = null, IConnectionManager connectionManager = null) :
            base(connectionManager)
        {
            Collation = collation;
            DatabaseName = databaseName;
        }

        /* ITask Interface */
        public override string TaskName => $"Create DB {DatabaseName}";
        public void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            var connectionManager = ConnectionManager.CloneWithMasterDbConnectionString();
            var ifDatabaseExists = new IfDatabaseExistsTask(DatabaseName)
            {
                DisableLogging = true,
                ConnectionManager = connectionManager
            };
            bool doesExist = ifDatabaseExists.Exists();
            if (doesExist)
                return;
            new SqlTask(this, Sql)
            {
                ConnectionManager = connectionManager
            }.
            ExecuteNonQuery();
        }

        /* Public properties */
        public string DatabaseName { get; set; }
        public RecoveryModel RecoveryModel { get; set; } = RecoveryModel.Simple;
        public string Collation { get; set; }
        public string Sql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                {
                    return
        $@"
USE [master]

CREATE DATABASE {QB}{DatabaseName}{QE} {CollationString} 
{RecoveryString}

--wait for database to enter 'ready' state
DECLARE @dbReady BIT = 0
WHILE (@dbReady = 0)
BEGIN
SELECT @dbReady = CASE WHEN DATABASEPROPERTYEX('{DatabaseName}', 'Collation') IS NULL THEN 0 ELSE 1 END                    
END
";
                }
                //else if (ConnectionType == ConnectionManagerType.MySql)
                //{
                //    return $@"CREATE DATABASE IF NOT EXISTS {DatabaseName} {CollationString}";
                //}
                else
                {
                    return $@"CREATE DATABASE {QB}{DatabaseName}{QE} {CollationString}";
                }
            }
        }

        /* Static methods for convenience */
        public static void Create(string databaseName) => new CreateDatabaseTask(databaseName).Execute();
        public static void Create(string databaseName, string collation) => new CreateDatabaseTask(databaseName, collation).Execute();
        public static void Create(IConnectionManager connectionManager, string databaseName)
            => new CreateDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, string databaseName, string collation)
            => new CreateDatabaseTask(databaseName, collation) { ConnectionManager = connectionManager }.Execute();

        /* Implementation & stuff */
        string RecoveryModelAsString
        {
            get
            {
                if (RecoveryModel == RecoveryModel.Simple)
                    return "SIMPLE";
                else if (RecoveryModel == RecoveryModel.BulkLogged)
                    return "BULK";
                else if (RecoveryModel == RecoveryModel.Full)
                    return "FULL";
                else return string.Empty;
            }
        }
        bool HasCollation => !String.IsNullOrWhiteSpace(Collation);
        string CollationString
        {
            get
            {
                if (!HasCollation) return string.Empty;
                if (ConnectionType == ConnectionManagerType.Postgres)
                    return "LC_COLLATE '" + Collation + "'";
                else
                    return "COLLATE " + Collation;
            }
        }
        string RecoveryString => RecoveryModel != RecoveryModel.Default ?
            $"ALTER DATABASE [{DatabaseName}] SET RECOVERY {RecoveryModelAsString} WITH no_wait"
            : string.Empty;

    }

    public enum RecoveryModel
    {
        Default, Simple, BulkLogged, Full
    }

}
