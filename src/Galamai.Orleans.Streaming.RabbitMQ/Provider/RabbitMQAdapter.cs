using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    internal class RabbitMQAdapter : IQueueAdapter
    {
        private readonly ConcurrentDictionary<(int, QueueId), IRabbitMQProducer> _queues =
            new ConcurrentDictionary<(int, QueueId), IRabbitMQProducer>();

        private readonly IBatchContainerSerializer _serializer;
        private readonly IStreamQueueMapper _mapper;
        private readonly IRabbitMQFactory _rmqFactory;
        private readonly TimeSpan _cacheFillingTimeout;

        public RabbitMQAdapter(
            RabbitMQOptions rmqOptions,
            CachingOptions cachingOptions,
            IBatchContainerSerializer serializer,
            IStreamQueueMapper mapper,
            string providerName,
            ILoggerFactory loggerFactory)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper)); ;
            Name = providerName;
            _rmqFactory = new RabbitMQFactory(rmqOptions, loggerFactory);
            _cacheFillingTimeout = cachingOptions.CacheFillingTimeout;
        }

        public string Name { get; }

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => (StreamProviderDirection)3;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return new RabbitMQAdapterReceiver(_rmqFactory, queueId, _serializer, _cacheFillingTimeout);
        }

        public Task QueueMessageBatchAsync<T>(
            Guid streamGuid,
            string streamNamespace,
            IEnumerable<T> events,
            StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (token != null)
                throw new ArgumentException("RabbitMq stream provider does not support non-null StreamSequenceToken.", nameof(token));

            var queueForStream = _mapper.GetQueueForStream(streamGuid, streamNamespace);
            var key = (Thread.CurrentThread.ManagedThreadId, queueForStream);
            if (!_queues.TryGetValue(key, out IRabbitMQProducer orAdd))
            {
                orAdd = _queues.GetOrAdd(key, _rmqFactory.CreateProducer(queueForStream));
            }

            orAdd.Send(RabbitMQDataAdapter.ToQueueMessage(_serializer, streamGuid, streamNamespace, events, requestContext));
            return Task.CompletedTask;
        }
    }
}
