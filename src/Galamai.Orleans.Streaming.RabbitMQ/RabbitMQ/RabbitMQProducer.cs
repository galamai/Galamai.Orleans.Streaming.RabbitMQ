using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    class RabbitMQProducer : IRabbitMQProducer, IDisposable
    {
        private readonly RabbitMQConnector _connection;

        public RabbitMQProducer(RabbitMQConnector connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Send(byte[] message)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqProducer: calling Send on thread {Thread.CurrentThread.Name}.");
                var basicProperties = _connection.Channel.CreateBasicProperties();
                basicProperties.MessageId = Guid.NewGuid().ToString();
                basicProperties.DeliveryMode = 2;
                _connection.Channel.BasicPublish(string.Empty, _connection.QueueName, true, basicProperties, message);
                _connection.Channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                throw new RabbitMQException("RabbitMqProducer: Send failed!", ex);
            }
        }
    }
}
