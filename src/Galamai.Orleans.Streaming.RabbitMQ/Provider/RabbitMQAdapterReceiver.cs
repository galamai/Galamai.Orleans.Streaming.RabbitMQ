using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    internal class RabbitMQAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRabbitMQFactory _rmqFactory;
        private readonly QueueId _queueId;
        private readonly IBatchContainerSerializer _serializer;
        private readonly TimeSpan _cacheFillingTimeout;
        private readonly ILogger _logger;
        private long _sequenceId;
        private IRabbitMQConsumer _consumer;

        public RabbitMQAdapterReceiver(
            IRabbitMQFactory rmqFactory,
            QueueId queueId,
            IBatchContainerSerializer serializer,
            TimeSpan cacheFillingTimeout)
        {
            _rmqFactory = rmqFactory;
            _queueId = queueId;
            _serializer = serializer;
            _cacheFillingTimeout = cacheFillingTimeout;
            _logger = rmqFactory.LoggerFactory.CreateLogger($"{typeof(RabbitMQAdapterReceiver).FullName}.{queueId}");
            _sequenceId = 0L;
        }

        public Task Initialize(TimeSpan timeout)
        {
            _consumer = _rmqFactory.CreateConsumer(_queueId);
            return Task.CompletedTask;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batch = new List<IBatchContainer>();
            var startTimestamp = DateTime.UtcNow;
            for (int count = 0; (count < maxCount || maxCount == -1) && !(DateTime.UtcNow - startTimestamp > _cacheFillingTimeout); ++count)
            {
                await Task.Yield();
                var basicGetResult = _consumer.Receive();
                if (basicGetResult != null)
                {
                    try
                    {
                        var mqBatchContainer = RabbitMQDataAdapter.FromQueueMessage(
                            _serializer,
                            basicGetResult.Body,
                            _sequenceId,
                            basicGetResult.DeliveryTag);

                        batch.Add(mqBatchContainer);
                        _sequenceId++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "GetQueueMessagesAsync: failed to deserialize the message! The message will be thrown away (by calling ACK).");
                        _consumer.Ack(basicGetResult.DeliveryTag);
                    }
                }
                else
                {
                    break;
                }
            }
            return batch;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            foreach(var message in messages)
            {
                var batchContainer = (RabbitMQBatchContainer)message;
                if (batchContainer.DeliveryFailure)
                {
                    _logger.LogDebug($"MessagesDeliveredAsync NACK #{batchContainer.DeliveryTag} {batchContainer.SequenceToken}");
                    _consumer.Nack(batchContainer.DeliveryTag);
                }
                else
                {
                    _logger.LogDebug($"MessagesDeliveredAsync ACK #{batchContainer.DeliveryTag} {batchContainer.SequenceToken}");
                    _consumer.Ack(batchContainer.DeliveryTag);
                }
            }

            return Task.CompletedTask;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            _consumer.Dispose();
            return Task.CompletedTask;
        }
    }
}
