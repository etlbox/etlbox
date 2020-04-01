using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Inserts, updates and (optionally) deletes data in db target.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// </code>
    /// </example>
    public class DbMerge<TInput> :
        DataFlowTransformation<TInput, TInput>,
        IDataFlowBatchDestination<TInput>,
        IDataFlowTransformation<TInput, TInput>
        where TInput : IMergeableRow, new()
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";

        public async Task ExecuteAsync() => await OutputSource.ExecuteAsync();
        public void Execute() => OutputSource.Execute();

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;
        public DeltaMode DeltaMode { get; set; }

        #region Table

        public TableDefinition TableDefinition { get; }
        public string TableName => tableName ?? TableDefinition?.Name;

        private readonly string tableName;

        #endregion

        public override IConnectionManager ConnectionManager
        {
            get => base.ConnectionManager;
            set
            {
                base.ConnectionManager = value;
                DestinationTableAsSource.ConnectionManager = value;
                DestinationTable.ConnectionManager = value;
            }
        }
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();
        public bool UseTruncateMethod
        {
            get
            {
                if (typeInfo.IdColumnNames == null || typeInfo.IdColumnNames?.Count == 0) return true;
                return _useTruncateMethod;
            }
            set
            {
                _useTruncateMethod = value;
            }
        }
        public int BatchSize
        {
            get => DestinationTable.BatchSize;
            set => DestinationTable.BatchSize = value;
        }

        /* Private stuff */
        bool _useTruncateMethod;

        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, ConnectionType);
        LookupTransformation<TInput, TInput> Lookup { get; set; }
        DbSource<TInput> DestinationTableAsSource { get; set; }
        DbDestination<TInput> DestinationTable { get; set; }
        List<TInput> InputData => Lookup.LookupData;
        Dictionary<string, TInput> InputDataDict { get; set; }
        CustomSource<TInput> OutputSource { get; set; }
        bool WasTruncationExecuted { get; set; }
        private readonly DBMergeTypeInfo typeInfo;

        public DbMerge(string tableName, int batchSize = DbDestination.DefaultBatchSize) :
            this(new TableDefinition(tableName), batchSize)
        { }

        public DbMerge(TableDefinition tableDefinition, int batchSize = DbDestination.DefaultBatchSize)
        {
            if (tableDefinition is null)
                throw new ArgumentNullException(nameof(tableDefinition));
            if (string.IsNullOrWhiteSpace(tableDefinition.Name))
                throw new ArgumentException("Table name is required");
            TableDefinition = tableDefinition;
            typeInfo = new DBMergeTypeInfo(typeof(TInput));
            DestinationTableAsSource = new DbSource<TInput>(ConnectionManager)
            {
                SourceTableDefinition = tableDefinition
            };
            DestinationTable = new DbDestination<TInput>(ConnectionManager, batchSize: batchSize)
            {
                DestinationTableDefinition = tableDefinition
            };
            InitInternalFlow();
            InitOutputFlow();
        }

        public DbMerge(IConnectionManager connectionManager, string tableName, int batchSize = DbDestination.DefaultBatchSize) :
            this(tableName, batchSize)
        {
            ConnectionManager = connectionManager;
        }

        public DbMerge(IConnectionManager connectionManager, TableDefinition tableDefinition, int batchSize = DbDestination.DefaultBatchSize) :
            this(tableDefinition, batchSize)
        {
            ConnectionManager = connectionManager;
        }

        private void InitInternalFlow()
        {
            Lookup = new LookupTransformation<TInput, TInput>(
                DestinationTableAsSource,
                row => UpdateRowWithDeltaInfo(row)
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {
                if (DeltaMode == DeltaMode.Delta)
                    DeltaTable.AddRange(batch.Where(row => row.ChangeAction != "D"));
                else
                    DeltaTable.AddRange(batch);

                if (!UseTruncateMethod)
                {
                    SqlDeleteIds(batch.Where(row => row.ChangeAction != "I" && row.ChangeAction != "E"));
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U").ToArray();
                }
                else
                {
                    TruncateDestinationOnce();
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U" || row.ChangeAction == "E").ToArray();
                }
            };

            Lookup.LinkTo(DestinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            OutputSource = new CustomSource<TInput>(() =>
            {
                return DeltaTable.ElementAt(x++);
            }, () => x >= DeltaTable.Count);

            DestinationTable.OnCompletion = () =>
            {
                IdentifyAndDeleteMissingEntries();
                OutputSource.Execute();
            };
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            if (InputDataDict == null) InitInputDataDictionary();
            row.ChangeDate = DateTime.Now;
            TInput find = default(TInput);
            InputDataDict.TryGetValue(row.UniqueId, out find);
            if (DeltaMode == DeltaMode.Delta && row.IsDeletion)
            {
                if (find != null)
                {
                    find.ChangeAction = "D";
                    row.ChangeAction = "D";
                }
            }
            else
            {
                row.ChangeAction = "I";
                //TInput find = InputData.Where(d => d.UniqueId == row.UniqueId).FirstOrDefault();
                if (find != null)
                {
                    if (row.Equals(find))
                    {
                        row.ChangeAction = "E";
                        find.ChangeAction = "E";
                    }
                    else
                    {
                        row.ChangeAction = "U";
                        find.ChangeAction = "U";
                    }
                }
            }
            return row;
        }

        private void InitInputDataDictionary()
        {
            InputDataDict = new Dictionary<string, TInput>();
            foreach (var d in InputData)
                InputDataDict.Add(d.UniqueId, d);
        }

        void TruncateDestinationOnce()
        {
            if (WasTruncationExecuted == true) return;
            WasTruncationExecuted = true;
            if (DeltaMode == DeltaMode.NoDeletions == true) return;
            TruncateTableTask.Truncate(this.ConnectionManager, TableName);
        }

        void IdentifyAndDeleteMissingEntries()
        {
            if (DeltaMode == DeltaMode.NoDeletions) return;
            IEnumerable<TInput> deletions = null;
            if (DeltaMode == DeltaMode.Delta)
                deletions = InputData.Where(row => row.ChangeAction == "D").ToList();
            else
                deletions = InputData.Where(row => String.IsNullOrEmpty(row.ChangeAction)).ToList();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            foreach (var row in deletions) //.ForEach(row =>
            {
                row.ChangeAction = "D";
                row.ChangeDate = DateTime.Now;
            };
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            var idsToDelete = rowsToDelete.Select(row => $"'{row.UniqueId}'");
            if (idsToDelete.Count() > 0)
            {
                string idNames = $"{QB}{typeInfo.IdColumnNames.First()}{QE}";
                if (typeInfo.IdColumnNames.Count > 1)
                    idNames = CreateConcatSqlForNames();
                new SqlTask(this, $@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {String.Join(",", idsToDelete)}
            )")
                {
                    DisableLogging = true,
                }.ExecuteNonQuery();
            }
        }

        private string CreateConcatSqlForNames()
        {
            string result = $"CONCAT( {string.Join(",", typeInfo.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", typeInfo.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
            return result;
        }

        public void Wait() => DestinationTable.Wait();
        public Task Completion => DestinationTable.Completion;
    }
}
