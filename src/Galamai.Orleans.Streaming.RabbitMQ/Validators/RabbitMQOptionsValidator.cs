using Galamai.Orleans.Streaming.RabbitMQ.Options;
using Orleans;
using Orleans.Runtime;

namespace Galamai.Orleans.Streaming.RabbitMQ.Validators
{
    public class RabbitMQOptionsValidator : IConfigurationValidator
    {
        private readonly RabbitMQOptions _options;
        private readonly string _name;

        public RabbitMQOptionsValidator(RabbitMQOptions options, string name)
        {
            _options = options;
            _name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(_options.HostName))
                MissingException(nameof(_options.HostName));

            if (string.IsNullOrEmpty(_options.VirtualHost))
                MissingException(nameof(_options.VirtualHost));

            if (string.IsNullOrEmpty(_options.UserName))
                MissingException(nameof(_options.UserName));

            if (string.IsNullOrEmpty(_options.Password))
                MissingException(nameof(_options.Password));

            if (string.IsNullOrEmpty(_options.QueueNamePrefix))
                MissingException(nameof(_options.QueueNamePrefix));

            if (_options.Port <= 0)
                NotPositiveException(nameof(_options.Port));
        }

        private void MissingException(string parameterName)
        {
            throw new OrleansConfigurationException($"Missing required parameter `{parameterName}` on stream provider {_name}.");
        }

        private void NotPositiveException(string parameterName)
        {
            throw new OrleansConfigurationException($"Value of parameter `{parameterName}` must be positive.");
        }
    }
}
