using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Truncates a table.
    /// </summary>
    /// <example>
    /// <code>
    /// TruncateTableTask.Truncate("demo.table1");
    /// </code>
    /// </example>
    public class TruncateTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Truncate table {TableName}";
        public void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string TableName { get; set; }
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);
        public string Sql
        {
            get
            {
                if (DbConnectionManager.SupportTableTruncate)
                    return $@"TRUNCATE TABLE {TN.QuotatedFullName}";
                else
                    return $@"DELETE FROM {TN.QuotatedFullName}";
            }
        }

        public TruncateTableTask()
        {

        }
        public TruncateTableTask(string tableName) : this()
        {
            this.TableName = tableName;
        }

        public static void Truncate(string tableName) => new TruncateTableTask(tableName).Execute();
        public static void Truncate(IConnectionManager connection, string tableName) => new TruncateTableTask(tableName) { ConnectionManager = connection }.Execute();


    }
}
