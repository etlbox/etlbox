using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Batching <see cref="IDataFlowDestination{TInput}"/>
    /// </summary>
    /// <typeparam name="TInput">Input type</typeparam>
    public interface IDataFlowBatchDestination<TInput> :
        IDataFlowDestination<TInput>
    {
        /// <summary>
        /// Batch size
        /// </summary>
        /// <value>positive</value>
        /// <exception cref="ArgumentOutOfRangeException">set: Value is not positive</exception>
        int BatchSize { get; set; }
    }
}
