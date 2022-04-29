using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQStandaloneSubscriptionClient : StandaloneRabbitMQClient, IMessageBusStandaloneConsumer, IDisposable
    {
        private IModel _channel;
        private IConnection _conn;
        private readonly string _subscriptionName;


        public RabbitMQStandaloneSubscriptionClient(ILogger<RabbitMQStandaloneSubscriptionClient> logger, string connectionString, string subscriptionName, int connectionAttempts = 8)
            : base(logger, connectionString, connectionAttempts)
        {
            _subscriptionName = subscriptionName;
        }

        public void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            _conn = GetConnection();
            _channel = CreateChannel(_conn);
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: _subscriptionName, autoAck: false, consumer: receiver);
        }

        public void Dispose()
        {
            _channel?.Close();
            _conn?.Close();
        }

        public Task Close()
        {
            _channel?.Close();
            _conn?.Close();

            return Task.CompletedTask;
        }
    }
}
