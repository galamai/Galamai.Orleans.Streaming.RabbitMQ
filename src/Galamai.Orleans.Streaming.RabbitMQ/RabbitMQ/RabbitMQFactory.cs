using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    class RabbitMQFactory : IRabbitMQFactory
    {
        private readonly RabbitMQOptions _options;

        public ILoggerFactory LoggerFactory { get; }

        public RabbitMQFactory(RabbitMQOptions options, ILoggerFactory loggerFactory)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            LoggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        }

        public IRabbitMQConsumer CreateConsumer(QueueId queueId)
        {
            var connector = new RabbitMQConnector(_options, queueId, LoggerFactory.CreateLogger($"{typeof(RabbitMQConsumer).FullName}.{queueId}"));
            return new RabbitMQConsumer(connector);
        }

        public IRabbitMQProducer CreateProducer(QueueId queueId)
        {
            var connector = new RabbitMQConnector(_options, queueId, LoggerFactory.CreateLogger($"{typeof(RabbitMQProducer).FullName}.{queueId}"));
            return new RabbitMQProducer(connector);
        }
    }
}
