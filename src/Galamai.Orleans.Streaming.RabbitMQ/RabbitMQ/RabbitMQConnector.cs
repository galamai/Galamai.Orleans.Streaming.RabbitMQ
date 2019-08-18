using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using RabbitMQ.Client;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    class RabbitMQConnector : IDisposable
    {
        private readonly RabbitMQOptions _options;
        private IConnection _connection;
        private IModel _channel;

        public IModel Channel
        {
            get
            {
                EnsureConnection();
                return _channel;
            }
        }

        public string QueueName { get; private set; }
        public ILogger Logger { get; private set; }

        public RabbitMQConnector(RabbitMQOptions options, QueueId queueId, ILogger logger)
        {
            _options = options;
            Logger = logger;
            QueueName = options.UseQueuePartitioning
                ? $"{options.QueueNamePrefix}-{queueId.GetNumericId()}"
                : options.QueueNamePrefix;
        }

        private void EnsureConnection()
        {
            if (_connection == null || !_connection.IsOpen)
            {
                Logger.LogDebug("Opening a new RMQ connection...");

                var factory = new ConnectionFactory()
                {
                    HostName = _options.HostName,
                    VirtualHost = _options.VirtualHost,
                    Port = _options.Port,
                    UserName = _options.UserName,
                    Password = _options.Password,
                    UseBackgroundThreadsForIO = false,
                    AutomaticRecoveryEnabled = false,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                };

                _connection = factory.CreateConnection();

                Logger.LogDebug("Connection created.");

                _connection.ConnectionShutdown += (connection, reason) => Logger.LogWarning($"Connection was shut down: [{reason.ReplyText}]");
                _connection.ConnectionBlocked += (connection, reason) => Logger.LogWarning($"Connection is blocked: [{reason.Reason}]");
                _connection.ConnectionUnblocked += (connection, args) => Logger.LogWarning("Connection is not blocked any more.");
            }

            if (_channel == null || !_channel.IsOpen)
            {
                Logger.LogDebug("Creating a model.");

                _channel = _connection.CreateModel();
                _channel.QueueDeclare(QueueName, true, false, false, null);
                _channel.ConfirmSelect();

                Logger.LogDebug("Model created.");
            }
        }

        public void Dispose()
        {
            try
            {
                _channel?.Close();
                _connection?.Close();
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during RMQ connection disposal.");
            }
        }
    }
}
