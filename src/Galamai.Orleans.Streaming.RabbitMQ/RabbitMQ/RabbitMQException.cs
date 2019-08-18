using System;

namespace Galamai.Orleans.Streaming.RabbitMQ.RabbitMQ
{
    [Serializable]
    public class RabbitMQException : Exception
    {
        public override string StackTrace { get; }

        public RabbitMQException(string message, Exception innerException)
          : base(message + " [" + innerException.Message + "]")
        {
            StackTrace = innerException.StackTrace;
        }

        public override string ToString()
        {
            return Message + Environment.NewLine + StackTrace;
        }
    }
}
