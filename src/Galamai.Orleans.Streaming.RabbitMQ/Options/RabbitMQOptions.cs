
using Microsoft.Extensions.Options;

namespace Galamai.Orleans.Streaming.RabbitMQ.Options
{
    public class RabbitMQOptions : IOptions<RabbitMQOptions>
    {
        public string HostName { get; set; }

        public int Port { get; set; }

        public string VirtualHost { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        public string QueueName { get; set; }

        public string QueueNamePrefix { get; set; }

        public bool UseQueuePartitioning { get; set; }

        public int NumberOfQueues { get; set; } = 1;

        public RabbitMQOptions Value => this;
    }
}
