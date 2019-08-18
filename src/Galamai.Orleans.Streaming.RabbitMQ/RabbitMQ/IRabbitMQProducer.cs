using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    interface IRabbitMQProducer : IDisposable
    {
        void Send(byte[] message);
    }
}
