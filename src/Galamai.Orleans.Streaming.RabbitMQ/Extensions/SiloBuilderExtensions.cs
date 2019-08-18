using Galamai.Orleans.Streaming.RabbitMQ.Containers;
using Galamai.Orleans.Streaming.RabbitMQ.Provider;
using Orleans.Hosting;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Extensions
{
    public static class SiloBuilderExtensions
    {
        public static ISiloHostBuilder AddRabbitMQStream<TSerializer>(
            this ISiloHostBuilder builder,
            string name,
            Action<SiloRabbitMQStreamConfigurator> configure)
            where TSerializer : IBatchContainerSerializer, new()
        {
            var streamConfigurator = new SiloRabbitMQStreamConfigurator(
                name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));

            configure?.Invoke(streamConfigurator);
            return builder;
        }
    }
}
