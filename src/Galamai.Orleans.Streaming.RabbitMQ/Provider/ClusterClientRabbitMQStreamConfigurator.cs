using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Galamai.Orleans.Streaming.RabbitMQ.Validators;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    public class ClusterClientRabbitMQStreamConfigurator : ClusterClientPersistentStreamConfigurator
    {
        public ClusterClientRabbitMQStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RabbitMQAdapterFactory.Create)
        {
            builder.ConfigureApplicationParts(parts =>
            {
                parts.AddFrameworkPart(typeof(RabbitMQAdapterFactory).Assembly);
                parts.AddFrameworkPart(typeof(EventSequenceTokenV2).Assembly);
                parts.AddFrameworkPart(typeof(EventSequenceToken).Assembly);
            });

            builder.ConfigureServices(services =>
            {
                services.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
                services.ConfigureNamedOptionForLogging<RabbitMQOptions>(name);
                services.AddTransient<IConfigurationValidator>(sp => new RabbitMQOptionsValidator(sp.GetOptionsByName<RabbitMQOptions>(name), name));
            });
        }

        public ClusterClientRabbitMQStreamConfigurator ConfigureRabbitMQ(
            string host,
            int port,
            string virtualHost,
            string user,
            string password,
            string queueName,
            bool useQueuePartitioning = false,
            int numberOfQueues = 1)
        {
            Configure<RabbitMQOptions>(ob => ob.Configure(options =>
            {
                options.HostName = host;
                options.Port = port;
                options.VirtualHost = virtualHost;
                options.UserName = user;
                options.Password = password;
                options.QueueNamePrefix = queueName;
                options.UseQueuePartitioning = useQueuePartitioning;
                options.NumberOfQueues = numberOfQueues;
            }));

            return this;
        }
    }
}
