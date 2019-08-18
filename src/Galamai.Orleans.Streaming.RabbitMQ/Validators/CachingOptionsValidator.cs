using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Orleans;
using Orleans.Runtime;
using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Validators
{
    public class CachingOptionsValidator : IConfigurationValidator
    {
        private readonly CachingOptions _options;

        public CachingOptionsValidator(CachingOptions options)
        {
            _options = options;
        }

        public void ValidateConfiguration()
        {
            if (_options.CacheSize <= 0)
                NotPositiveException(nameof(_options.CacheSize));

            if (_options.CacheFillingTimeout <= TimeSpan.Zero)
                NotPositiveException(nameof(_options.CacheFillingTimeout));
        }

        private void NotPositiveException(string parameterName)
        {
            throw new OrleansConfigurationException($"Value of parameter `{parameterName}` must be positive.");
        }
    }
}
