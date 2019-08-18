using Orleans.Serialization;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Containers
{
    public class DefaultBatchContainerSerializer : IBatchContainerSerializer
    {
        private readonly SerializationManager _serializatonManager;

        public DefaultBatchContainerSerializer()
        {
            throw new NotImplementedException("Call \"DefaultBatchContainerSerializer(SerializationManager)\" instead.");
        }

        public DefaultBatchContainerSerializer(SerializationManager serializationManager)
        {
            _serializatonManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
        }

        public RabbitMQBatchContainer Deserialize(byte[] data)
        {
            return _serializatonManager.DeserializeFromByteArray<RabbitMQBatchContainer>(data);
        }

        public byte[] Serialize(RabbitMQBatchContainer container)
        {
            return _serializatonManager.SerializeToByteArray(container);
        }
    }
}
