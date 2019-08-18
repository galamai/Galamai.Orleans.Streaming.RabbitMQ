using RabbitMQ.Client;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    interface IRabbitMQConsumer : IDisposable
    {
        void Ack(ulong deliveryTag);
        void Nack(ulong deliveryTag);
        BasicGetResult Receive();
    }
}
