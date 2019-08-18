using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Orleans.Streams;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Caches
{
    public class ConcurrentQueueCacheCursor : IQueueCacheCursor, IDisposable
    {
        private readonly object syncRoot = new object();
        private readonly Func<RabbitMQBatchContainer> _moveNext;
        private readonly Action<RabbitMQBatchContainer> _purgeItem;

        private RabbitMQBatchContainer _current;

        public ConcurrentQueueCacheCursor(Func<RabbitMQBatchContainer> moveNext, Action<RabbitMQBatchContainer> purgeItem)
        {
            _moveNext = moveNext ?? throw new ArgumentNullException(nameof(moveNext));
            _purgeItem = purgeItem ?? throw new ArgumentNullException(nameof(purgeItem));
        }

        public void Dispose()
        {
            lock (syncRoot)
            {
                _purgeItem(_current);
                _current = null;
            }
        }

        public IBatchContainer GetCurrent(out Exception exception)
        {
            exception = null;
            return _current;
        }

        public bool MoveNext()
        {
            lock (syncRoot)
            {
                _purgeItem(_current);
                _current = _moveNext();
            }
            return _current != null;
        }

        public void Refresh(StreamSequenceToken token)
        {
        }

        public void RecordDeliveryFailure()
        {
            if (_current == null)
                return;

            _current.DeliveryFailure = true;
        }
    }
}
