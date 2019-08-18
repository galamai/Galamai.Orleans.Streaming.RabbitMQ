using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    interface IRabbitMQFactory
    {
        ILoggerFactory LoggerFactory { get; }
        IRabbitMQConsumer CreateConsumer(QueueId queueId);
        IRabbitMQProducer CreateProducer(QueueId queueId);
    }
}
