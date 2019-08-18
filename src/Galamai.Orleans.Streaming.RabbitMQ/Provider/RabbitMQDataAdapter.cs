using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Orleans.Providers.Streams.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    public static class RabbitMQDataAdapter
    {
        public static RabbitMQBatchContainer FromQueueMessage(
            IBatchContainerSerializer serializer,
            byte[] data,
            long sequenceId,
            ulong deliveryTag)
        {
            var mqBatchContainer = serializer.Deserialize(data);
            mqBatchContainer.EventSequenceToken = new EventSequenceToken(sequenceId);
            mqBatchContainer.DeliveryTag = deliveryTag;
            return mqBatchContainer;
        }

        public static byte[] ToQueueMessage<T>(
            IBatchContainerSerializer serializer,
            Guid streamGuid,
            string streamNamespace,
            IEnumerable<T> events,
            Dictionary<string, object> requestContext)
        {
            var container = new RabbitMQBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            return serializer.Serialize(container);
        }
    }
}
