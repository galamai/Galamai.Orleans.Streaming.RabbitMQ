using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Galamai.Orleans.Streaming.RabbitMQ.Containers
{
    [Serializable]
    public class RabbitMQBatchContainer : IBatchContainer
    {
        private readonly List<object> _events;
        private readonly Dictionary<string, object> _requestContext;

        public EventSequenceToken EventSequenceToken { set; private get; }

        public StreamSequenceToken SequenceToken => EventSequenceToken;

        public Guid StreamGuid { get; }

        public string StreamNamespace { get; }

        public ulong DeliveryTag { get; set; }

        public bool DeliveryFailure { get; set; }

        public RabbitMQBatchContainer(
            Guid streamGuid,
            string streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext)
        {
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _events = events;
            _requestContext = requestContext;
        }

        public bool ImportRequestContext()
        {
            if (_requestContext == null)
                return false;

            RequestContextExtensions.Import(_requestContext);

            return true;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>()
                .Select((e, i) => Tuple.Create(e, (StreamSequenceToken)EventSequenceToken?.CreateSequenceTokenForEvent(i)))
                .ToList();
        }

        public bool ShouldDeliver(
            IStreamIdentity stream,
            object filterData,
            StreamFilterPredicate shouldReceiveFunc)
        {
            return _events.Any(item => shouldReceiveFunc(stream, filterData, item));
        }
    }
}
