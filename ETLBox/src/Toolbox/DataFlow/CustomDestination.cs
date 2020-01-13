using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class CustomDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        private Task targetActionCompletionTask;
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Write data into custom target";

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => TargetActionBlock;
        public Action<TInput> WriteAction
        {
            get
            {
                return _writeAction;
            }
            set
            {
                _writeAction = value;
                TargetActionBlock = new ActionBlock<TInput>(AddLogging(_writeAction));
                targetActionCompletionTask = TargetActionBlock.Completion.ContinueWith(t => CleanUp());
            }
        }

        public Action OnCompletion { get; set; }

        /* Private stuff */
        private Action<TInput> _writeAction;

        internal ActionBlock<TInput> TargetActionBlock { get; set; }
        public CustomDestination()
        {
        }

        public CustomDestination(Action<TInput> writeAction) : this()
        {
            WriteAction = writeAction;
        }

        internal CustomDestination(ITask callingTask, Action<TInput> writeAction) : this(writeAction)
        {
            CopyTaskProperties(callingTask);
        }

        public CustomDestination(string taskName, Action<TInput> writeAction) : this(writeAction)
        {
            this.TaskName = taskName;
        }

        public void Wait()
        {
            targetActionCompletionTask.Wait();
        }

        public Task Completion => targetActionCompletionTask;

        private void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }

        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        private Action<TInput> AddLogging(Action<TInput> writeAction)
        {
            return new Action<TInput>(
                input =>
                {
                    if (ProgressCount == 0) NLogStart();
                    writeAction.Invoke(input);
                    LogProgress(1);
                });
        }
    }

    /// <summary>
    /// Define your own destination block. The non generic implementation accepts a string array as input.
    /// </summary>
    public class CustomDestination : CustomDestination<string[]>
    {
        public CustomDestination() : base()
        { }

        public CustomDestination(Action<string[]> writeAction) : base(writeAction)
        { }
    }
}
