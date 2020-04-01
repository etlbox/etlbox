using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// DbDestination&lt;MyRow&gt; dest = new DbDestination&lt;MyRow&gt;("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DbDestination<TInput> :
        DataFlowBatchDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        public DbDestination(TableDefinition tableDefinition, IConnectionManager connectionManager = null, int batchSize = DefaultBatchSize) :
            base(connectionManager, batchSize)
        {
            TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));
            typeInfo = new TypeInfo(typeof(TInput));
        }

        /* ITask Interface */
        public override string TaskName => $"Write data into table {TableDefinition.Name}";
        /* Public properties */
        public TableDefinition TableDefinition { get; }

        private readonly TypeInfo typeInfo;

        protected override void WriteBatch(ref TInput[] data)
        {
            TableDefinition.EnsureColumns(DbConnectionManager);

            base.WriteBatch(ref data);

            TryBulkInsertData(data);

            LogProgressBatch(data.Length);
        }

        private void TryBulkInsertData(TInput[] data)
        {
            TableData<TInput> td = CreateTableDataObject(ref data);
            try
            {
                new SqlTask(this, $"Execute Bulk insert")
                {
                    DisableLogging = true
                }
                .BulkInsert(td, TableDefinition.Name);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TInput[]>(data));
            }
        }

        private TableData<TInput> CreateTableDataObject(ref TInput[] data)
        {
            var rows = ConvertRows(ref data);
            var td = new TableData<TInput>(TableDefinition, rows);
            if (typeInfo.IsDynamic && data.Length > 0)
                td.DynamicColumnNames.AddRange(((IDictionary<string, object>)data[0]).Keys);
            return td;
        }

        private List<object[]> ConvertRows(ref TInput[] data)
        {
            List<object[]> result = new List<object[]>(data.Length);
            foreach (var CurrentRow in data)
            {
                if (CurrentRow == null) continue;
                object[] rowResult;
                if (typeInfo.IsArray)
                {
                    rowResult = CurrentRow as object[];
                }
                else if (typeInfo.IsDynamic)
                {
                    IDictionary<string, object> propertyValues = (IDictionary<string, object>)CurrentRow;
                    rowResult = new object[propertyValues.Count];
                    int index = 0;
                    foreach (var prop in propertyValues)
                    {
                        rowResult[index] = prop.Value;
                        index++;
                    }
                }
                else
                {
                    rowResult = new object[typeInfo.PropertyLength];
                    int index = 0;
                    foreach (PropertyInfo propInfo in typeInfo.Properties)
                    {
                        rowResult[index] = propInfo.GetValue(CurrentRow);
                        index++;
                    }
                }
                result.Add(rowResult);
            }
            return result;
        }
    }

    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The DbDestination uses a dynamic object as input type. If you need other data types, use the generic DbDestination instead.
    /// </summary>
    /// <see cref="DbDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic DbDestination works with dynamic object as input
    /// //use DbDestination&lt;TInput&gt; for generic usage!
    /// DbDestination dest = new DbDestination("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DbDestination :
        DbDestination<ExpandoObject>
    {
        public DbDestination(TableDefinition tableDefinition, IConnectionManager connectionManager = null, int batchSize = DefaultBatchSize) :
            base(tableDefinition, connectionManager, batchSize)
        { }
    }
}
