using System.Collections.Concurrent;

namespace DotNet.Utils.Reactive
{
    public class BufferSynchronizationContext : SynchronizationContext
    {
        private readonly BlockingCollection<(SendOrPostCallback d, object? state)> _queue;
        private readonly CancellationToken _cancellationToken;

        public BufferSynchronizationContext(int capacity, CancellationToken cancellationToken = default)
        {
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
            _queue = new BlockingCollection<(SendOrPostCallback d, object? state)>(capacity);
            _cancellationToken = cancellationToken;
        }

        public override void OperationCompleted()
        {
            _queue?.Dispose();
        }

        public override void OperationStarted()
        {
            Task.Run(() =>
            {
                while (!_cancellationToken.IsCancellationRequested)
                {
                    var (d, state) = _queue.Take(_cancellationToken);
                    base.Send(d, state);
                }
            }, _cancellationToken).ConfigureAwait(false);
        }

        public override void Post(SendOrPostCallback d, object? state)
        {
            _queue.Add((d, state));
        }
    }
}
