using System.Threading.Tasks;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowDestination<TInput> : IDataFlowLinkTarget<TInput>
    {
        void Wait();
        Task Completion { get; }
    }
}
