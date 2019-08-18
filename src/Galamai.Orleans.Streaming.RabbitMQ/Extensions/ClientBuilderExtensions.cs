using Galamai.Orleans.Streaming.RabbitMQ.Provider;
using Orleans;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Extensions
{
    public static class ClientBuilderExtensions
    {
        public static IClientBuilder AddRabbitMQStream(
            this IClientBuilder builder,
            string name,
            Action<ClusterClientRabbitMQStreamConfigurator> configure)
        {
            var streamConfigurator = new ClusterClientRabbitMQStreamConfigurator(name, builder);
            configure?.Invoke(streamConfigurator);
            return builder;
        }
    }
}
