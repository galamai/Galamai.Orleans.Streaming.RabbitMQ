using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.Options
{
    public class CachingOptions
    {
        public int CacheSize { get; set; } = 4096;
        public TimeSpan CacheFillingTimeout { get; set; } = TimeSpan.FromSeconds(10);
    }
}
