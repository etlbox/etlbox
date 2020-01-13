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
    public class DBMerge<TInput> : DataFlowTask, ITask, IDataFlowLinkTarget<TInput>, IDataFlowSource<TInput> where TInput : IMergeableRow, new()
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";

        public async Task ExecuteAsync() => await OutputSource.ExecuteAsync();
        public void Execute() => OutputSource.Execute();

        /* Public Properties */
        public ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;
        public bool DisableDeletion { get; set; }
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, ConnectionType);
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();
        public bool UseTruncateMethod
        {
            get
            {
                if (MergeIdColumnNames == null || MergeIdColumnNames?.Count == 0) return true;
                return _useTruncateMethod;
            }
            set
            {
                _useTruncateMethod = value;
            }
        }

        /* Private stuff */
        bool _useTruncateMethod;
        Lookup<TInput, TInput, TInput> Lookup { get; set; }
        DBSource<TInput> DestinationTableAsSource { get; set; }
        DBDestination<TInput> DestinationTable { get; set; }
        List<TInput> InputData { get; set; } = new List<TInput>();
        CustomSource<TInput> OutputSource { get; set; }
        bool WasDeletionExecuted { get; set; }
        List<string> MergeIdColumnNames { get; set; }

        public DBMerge(string tableName)
        {
            TableName = tableName;
            DestinationTableAsSource = new DBSource<TInput>(TableName);
            DestinationTable = new DBDestination<TInput>(TableName);
            Init();
        }

        public DBMerge(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            TableName = tableName;
            ConnectionManager = connectionManager;
            DestinationTableAsSource = new DBSource<TInput>(connectionManager, TableName);
            DestinationTable = new DBDestination<TInput>(connectionManager, TableName);
            Init();
        }

        public DBMerge(TableDefinition tableDefinition)
        {
            DestinationTableDefinition = tableDefinition;
            TableName = tableDefinition.Name;
            DestinationTableAsSource = new DBSource<TInput>()
            {
                SourceTableDefinition = DestinationTableDefinition
            };
            Init();
        }

        private void Init()
        {
            GetIdColumName();
            InitInternalFlow();
            InitOutputFlow();
        }

        private void GetIdColumName()
        {
            TypeInfo typeInfo = new TypeInfo(typeof(TInput));
            MergeIdColumnNames = typeInfo.IdColumnNames;
        }


        private void InitInternalFlow()
        {
            Lookup = new Lookup<TInput, TInput, TInput>(
                row => UpdateRowWithDeltaInfo(row),
                DestinationTableAsSource,
                InputData
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {
                DeleteMissingEntriesOnce();
                DeltaTable.AddRange(batch);
                if (!UseTruncateMethod)
                    SqlDeleteIds(batch.Where(row => row.ChangeAction != "I" && row.ChangeAction != "E"));
                if (UseTruncateMethod)
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U" || row.ChangeAction == "E").ToArray();
                else
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U").ToArray();
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

            DestinationTable.OnCompletion = () => OutputSource.Execute();
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            row.ChangeDate = DateTime.Now;
            row.ChangeAction = "I";
            TInput find = InputData.Where(d => d.UniqueId == row.UniqueId).FirstOrDefault();
            if (find != null)
            {
                if (row.Equals(find))
                {
                    row.ChangeAction = "E";
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = "E";
                }
                else
                {
                    row.ChangeAction = "U";
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = "U";
                }
            }

            return row;
        }

        void DeleteMissingEntriesOnce()
        {
            if (WasDeletionExecuted == true) return;
            WasDeletionExecuted = true;
            if (DisableDeletion == true) return;
            var deletions = InputData.Where(row => String.IsNullOrEmpty(row.ChangeAction));
            if (UseTruncateMethod)
                TruncateTableTask.Truncate(this.ConnectionManager, TableName);
            else
                SqlDeleteIds(deletions);
            DeltaTable.AddRange(deletions);
            DeltaTable.ForEach(row =>
            {
                row.ChangeAction = "D";
                row.ChangeDate = DateTime.Now;
            });
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            var idsToDelete = rowsToDelete.Select(row => $"'{row.UniqueId}'");
            if (idsToDelete.Count() > 0)
            {
                string idNames = $"{QB}{MergeIdColumnNames.First()}{QE}";
                if (MergeIdColumnNames.Count > 1)
                    idNames = CreateConcatSqlForNames();
                new SqlTask(this, $@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {String.Join(",", idsToDelete)}
            )")
                {
                    DisableLogging = true,
                    DisableExtension = true,
                }.ExecuteNonQuery();
            }
        }

        private string CreateConcatSqlForNames()
        {
            string result =  $"CONCAT( {string.Join(",", MergeIdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", MergeIdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
            return result;
        }

        public void Wait() => DestinationTable.Wait();
        public async Task Completion() => await DestinationTable.Completion;

        public IDataFlowLinkSource<TInput> LinkTo(IDataFlowLinkTarget<TInput> target)
            => OutputSource.LinkTo(target);

        public IDataFlowLinkSource<TInput> LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate)
            => OutputSource.LinkTo(target, predicate);

        public IDataFlowLinkSource<TInput> LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> rowsToKeep, Predicate<TInput> rowsIntoVoid)
            => OutputSource.LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TInput> target)
            => OutputSource.LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate)
            => OutputSource.LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TInput> target, Predicate<TInput> rowsToKeep, Predicate<TInput> rowsIntoVoid)
            => OutputSource.LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        public ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;

    }


}
