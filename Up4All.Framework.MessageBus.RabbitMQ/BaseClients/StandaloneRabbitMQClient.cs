using Microsoft.Extensions.Logging;

using Polly;

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;

namespace Up4All.Framework.MessageBus.RabbitMQ.BaseClients
{
    public abstract class StandaloneRabbitMQClient : MessageBusStandaloneClientBase, IDisposable
    {
        private IConnection _conn;
        private readonly ILogger<StandaloneRabbitMQClient> _logger;
        private readonly string _connectionString;
        private readonly int _connectionAttempts;


        public StandaloneRabbitMQClient(ILogger<StandaloneRabbitMQClient> logger, string connectionString, int connectionAttempts = 8)
        {
            _logger = logger;
            _connectionString = connectionString;
            _connectionAttempts = connectionAttempts;
        }

        public IConnection GetConnection()
        {
            var result = Policy
                .Handle<BrokerUnreachableException>()
                .WaitAndRetry(_connectionAttempts, retryAttempt =>
                {
                    TimeSpan wait = TimeSpan.FromSeconds(Math.Pow(2, retryAttempt));
                    _logger.LogInformation($"Failed to connect in RabbitMQ server, retrying in {wait}");
                    return wait;
                })
                .ExecuteAndCapture(() =>
                {
                    if (_conn != null && _conn.IsOpen) return;

                    _logger.LogDebug($"Trying to connect in RabbitMQ server");
                    _conn = new ConnectionFactory() { Uri = new Uri(_connectionString) }.CreateConnection();
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
