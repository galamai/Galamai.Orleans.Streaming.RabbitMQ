using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Galamai.Orleans.Streaming.RabbitMQ.Validators;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Provider
{
    public class SiloRabbitMQStreamConfigurator : SiloPersistentStreamConfigurator
    {
        public SiloRabbitMQStreamConfigurator(
          string name,
          Action<Action<IServiceCollection>> configureDelegate,
          Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureDelegate, new Func<IServiceProvider, string, IQueueAdapterFactory>(RabbitMQAdapterFactory.Create))
        {
            configureAppPartsDelegate?.Invoke(parts =>
            {
                parts.AddFrameworkPart(typeof(RabbitMQAdapterFactory).Assembly);
                parts.AddFrameworkPart(typeof(EventSequenceTokenV2).Assembly);
                parts.AddFrameworkPart(typeof(EventSequenceToken).Assembly);
            });

            configureDelegate?.Invoke(services =>
            {
                services.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
                services.ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name);
                services.ConfigureNamedOptionForLogging<RabbitMQOptions>(name);
                services.AddTransient<IConfigurationValidator>(sp => new RabbitMQOptionsValidator(sp.GetOptionsByName<RabbitMQOptions>(name), name));
                services.AddTransient<IConfigurationValidator>(sp => new SimpleQueueCacheOptionsValidator(sp.GetOptionsByName<SimpleQueueCacheOptions>(name), name));
            });
        }

        public SiloRabbitMQStreamConfigurator ConfigureRabbitMQ(
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

        public SiloRabbitMQStreamConfigurator ConfigureCache(int cacheSize)
        {
            Configure<CachingOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public SiloRabbitMQStreamConfigurator ConfigureCache(int cacheSize, TimeSpan cacheFillingTimeout)
        {
            Configure<CachingOptions>(ob => ob.Configure(options =>
            {
                options.CacheSize = cacheSize;
                options.CacheFillingTimeout = cacheFillingTimeout;
            }));
            return this;
        }
    }
}
