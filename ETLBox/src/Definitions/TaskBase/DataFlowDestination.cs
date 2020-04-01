using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        protected DataFlowDestination(IConnectionManager connectionManager = null) :
            base(connectionManager)
        { }

        public Action OnCompletion { get; set; }
        public Task Completion { get; protected set; }
        public ITargetBlock<TInput> TargetBlock => TargetAction;
        public virtual void Wait() => Completion.Wait();

        protected ActionBlock<TInput> TargetAction { get; set; }
        protected List<Task> PredecessorCompletions { get; set; } = new List<Task>();
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
             => ErrorHandler.LinkErrorTo(target, TargetAction.Completion);

        protected void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
            {
                if (!TargetBlock.Completion.IsCompleted)
                {
                    if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
                    else TargetBlock.Complete();
                }
            });
        }

        protected void SetCompletionTask() => Completion = AwaitCompletion();

        protected virtual async Task AwaitCompletion()
        {
            try
            {
                await TargetAction.Completion.ConfigureAwait(false);
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                CleanUp();
            }
        }

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }
    }
}
