using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Polly;

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Options;

namespace Up4All.Framework.MessageBus.RabbitMQ.BaseClients
{
    public abstract class RabbitMQClient : MessageBusClientBase, IDisposable
    {
        private IConnection _conn;        
        private readonly ILogger<RabbitMQClient> _logger;

        public RabbitMQClient(ILogger<RabbitMQClient> logger, IOptions<MessageBusOptions> messageBusOptions) : base(messageBusOptions)
        {
            _logger = logger;
        }

        public IConnection GetConnection()
        {
            if (_conn != null) return _conn;

            var result = Policy
                .Handle<BrokerUnreachableException>()
                .WaitAndRetry(MessageBusOptions.ConnectionAttempts, retryAttempt =>
                {
                    TimeSpan wait = TimeSpan.FromSeconds(Math.Pow(2, retryAttempt));
                    _logger.LogInformation($"Failed to connect in RabbitMQ server, retrying in {wait}");
                    return wait;
                })
                .ExecuteAndCapture(() =>
                {
                    _logger.LogDebug($"Trying to connect in RabbitMQ server");
                    _conn = new ConnectionFactory() { Uri = new Uri(MessageBusOptions.ConnectionString) }.CreateConnection();                    
                });

            if (result.Outcome != OutcomeType.Successful)
                throw result.FinalException;

            return _conn;
        }

        public IModel CreateChannel(IConnection conn)
        {
            return conn.CreateModel();
        }

        public void Dispose()
        {
            _conn?.Close();
        }

        public Task Close()
        {
            _conn?.Close();

            return Task.CompletedTask;
        }
    }
}
