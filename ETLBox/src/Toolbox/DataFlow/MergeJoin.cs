using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for input block one.</typeparam>
    /// <typeparam name="TInput2">Type of data for input block two.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt; join = new MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt;(Func&lt;TInput1, TInput2, TOutput&gt; mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput1, TInput2, TOutput> : DataFlowTask, ITask, IDataFlowLinkSource<TOutput>
    {
        private Func<TInput1, TInput2, TOutput> _mergeJoinFunc;

        /* ITask Interface */
        public override string TaskName { get; set; } = "Merge and join data";

        /* Public Properties */
        public MergeJoinTarget<TInput1> Target1 { get; set; }
        public MergeJoinTarget<TInput2> Target2 { get; set; }
        public ISourceBlock<TOutput> SourceBlock => Transformation.SourceBlock;

        public Func<TInput1, TInput2, TOutput> MergeJoinFunc
        {
            get { return _mergeJoinFunc; }
            set
            {
                _mergeJoinFunc = value;
                Transformation.RowTransformationFunc = new Func<Tuple<TInput1, TInput2>, TOutput>(tuple => _mergeJoinFunc.Invoke(tuple.Item1, tuple.Item2));
                JoinBlock.LinkTo(Transformation.TargetBlock, new DataflowLinkOptions { PropagateCompletion = true });
            }
        }

        /* Private stuff */
        internal BufferBlock<TInput1> Buffer1 { get; set; }
        internal BufferBlock<TInput1> Buffer2 { get; set; }
        internal JoinBlock<TInput1, TInput2> JoinBlock { get; set; }
        internal RowTransformation<Tuple<TInput1, TInput2>, TOutput> Transformation { get; set; }

        public MergeJoin()
        {
            Transformation = new RowTransformation<Tuple<TInput1, TInput2>, TOutput>(this);
            JoinBlock = new JoinBlock<TInput1, TInput2>();
            Target1 = new MergeJoinTarget<TInput1>(this, JoinBlock.Target1);
            Target2 = new MergeJoinTarget<TInput2>(this, JoinBlock.Target2);
        }

        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this()
        {
            MergeJoinFunc = mergeJoinFunc;
        }

        public MergeJoin(string name, Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this(mergeJoinFunc)
        {
            this.TaskName = name;
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);
    }

    public class MergeJoinTarget<TInput> : GenericTask, IDataFlowDestination<TInput>
    {
        public ITargetBlock<TInput> TargetBlock { get; set; }

        public void Wait()
        {
            TargetBlock.Completion.Wait();
        }

        public Task Completion => TargetBlock.Completion;

        public MergeJoinTarget(ITask parent, ITargetBlock<TInput> joinTarget)
        {
            TargetBlock = joinTarget;
            CopyTaskProperties(parent);

        }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput">Type of data for both inputs and output.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow&gt; join = new MergeJoin&lt;MyDataRow&gt;(mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput> : MergeJoin<TInput, TInput, TInput>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<TInput, TInput, TInput> mergeJoinFunc) : base(mergeJoinFunc)
        { }

        public MergeJoin(string name, Func<TInput, TInput, TInput> mergeJoinFunc) : base(name, mergeJoinFunc)
        { }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base.
    /// Make sure both inputs are sorted or in the right order. The non generic implementation deals with
    /// string array as inputs and merged output.
    /// </summary>
    public class MergeJoin : MergeJoin<string[], string[], string[]>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<string[], string[], string[]> mergeJoinFunc) : base(mergeJoinFunc)
        { }

        public MergeJoin(string name,Func<string[], string[], string[]> mergeJoinFunc) : base(name, mergeJoinFunc)
        { }
    }
}

