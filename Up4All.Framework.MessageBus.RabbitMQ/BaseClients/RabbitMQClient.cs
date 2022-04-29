using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Polly;

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

using System;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Options;

namespace Up4All.Framework.MessageBus.RabbitMQ.BaseClients
{
    public abstract class RabbitMQClient : MessageBusClientBase
    {
        private readonly ILogger<RabbitMQClient> _logger;


        public RabbitMQClient(ILogger<RabbitMQClient> logger, IOptions<MessageBusOptions> messageBusOptions) : base(messageBusOptions)
        {
            _logger = logger;
        }

        public IConnection GetConnection()
        {
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
                    return new ConnectionFactory() { Uri = new Uri(MessageBusOptions.ConnectionString) }.CreateConnection();
                });

            if (result.Outcome == OutcomeType.Successful)
                return result.Result;

            throw result.FinalException;
        }

        public IModel CreateChannel(IConnection conn)
        {
            return conn.CreateModel();
        }
    }
}
