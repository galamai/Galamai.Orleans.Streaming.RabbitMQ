using Galamai.Orleans.Streaming.RabbitMQ.Provider;
using Orleans.Hosting;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Extensions
{
    public static class SiloBuilderExtensions
    {
        public static ISiloHostBuilder AddRabbitMQStream(
            this ISiloHostBuilder builder,
            string name,
            Action<SiloRabbitMQStreamConfigurator> configure)
        {
            var streamConfigurator = new SiloRabbitMQStreamConfigurator(
                name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));

            configure?.Invoke(streamConfigurator);
            return builder;
        }

        public static ISiloBuilder AddRabbitMQStream(
            this ISiloBuilder siloBuilder,
            string name,
            Action<SiloRabbitMQStreamConfigurator> configure)
        {
            var streamConfigurator = new SiloRabbitMQStreamConfigurator(
                name,
                configureServicesDelegate => siloBuilder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => siloBuilder.ConfigureApplicationParts(configureAppPartsDelegate));

            configure?.Invoke(streamConfigurator);
            return siloBuilder;
        }
    }
}
