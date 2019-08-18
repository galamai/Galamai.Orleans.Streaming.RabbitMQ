using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Threading;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    class RabbitMQConsumer : IRabbitMQConsumer, IDisposable
    {
        private readonly RabbitMQConnector _connection;

        public RabbitMQConsumer(RabbitMQConnector connection)
        {
            _connection = connection;
        }

        public void Ack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqConsumer: calling Ack on thread {Thread.CurrentThread.Name}.");
                _connection.Channel.BasicAck(deliveryTag, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call ACK!");
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Nack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqConsumer: calling Nack on thread {Thread.CurrentThread.Name}.");
                _connection.Channel.BasicNack(deliveryTag, false, true);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call NACK!");
            }
        }

        public BasicGetResult Receive()
        {
            try
            {
                return _connection.Channel.BasicGet(_connection.QueueName, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call Get!");
                return null;
            }
        }
    }
}
