
namespace Galamai.Orleans.Streaming.RabbitMQ.Containers
{
    public interface IBatchContainerSerializer
    {
        byte[] Serialize(RabbitMQBatchContainer container);
        RabbitMQBatchContainer Deserialize(byte[] data);
    }
}
