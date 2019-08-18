using Galamai.Orleans.Streaming.RabbitMQ.Caches;
using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Threading.Tasks;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    public class RabbitMQAdapterFactory : IQueueAdapterFactory
    {
        private readonly IQueueAdapterCache _cache;
        private readonly IStreamQueueMapper _mapper;
        private readonly Task<IStreamFailureHandler> _failureHandler;
        private readonly IQueueAdapter _adapter;

        public RabbitMQAdapterFactory(
            string providerName,
            RabbitMQOptions rmqOptions,
            CachingOptions cachingOptions,
            IServiceProvider serviceProvider,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(providerName))
                throw new ArgumentNullException(nameof(providerName));
            if (rmqOptions == null)
                throw new ArgumentNullException(nameof(rmqOptions));
            if (cachingOptions == null)
                throw new ArgumentNullException(nameof(cachingOptions));
            if (serviceProvider == null)
                throw new ArgumentNullException(nameof(serviceProvider));
            if (loggerFactory == null)
                throw new ArgumentNullException(nameof(loggerFactory));

            _cache = new ConcurrentQueueAdapterCache(cachingOptions.CacheSize);

            var queueMapperOptions = new HashRingStreamQueueMapperOptions()
            {
                TotalQueueCount = rmqOptions.NumberOfQueues
            };

            _mapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, rmqOptions.QueueNamePrefix);
            _failureHandler = Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));
            var serializer = new DefaultBatchContainerSerializer(serviceProvider.GetRequiredService<SerializationManager>());
            _adapter = new RabbitMQAdapter(rmqOptions, cachingOptions, serializer, _mapper, providerName, loggerFactory);
        }

        public Task<IQueueAdapter> CreateAdapter() => Task.FromResult(_adapter);

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => _failureHandler;

        public IQueueAdapterCache GetQueueAdapterCache() => _cache;

        public IStreamQueueMapper GetStreamQueueMapper() => _mapper;

        public static RabbitMQAdapterFactory Create(IServiceProvider provider, string name)
        {
            return ActivatorUtilities.CreateInstance<RabbitMQAdapterFactory>(provider, name, provider.GetOptionsByName<RabbitMQOptions>(name), provider.GetOptionsByName<CachingOptions>(name));
        }
    }
}
