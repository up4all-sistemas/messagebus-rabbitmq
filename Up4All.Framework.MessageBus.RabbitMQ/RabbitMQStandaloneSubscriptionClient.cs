using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;
using Up4All.Framework.MessageBus.RabbitMQ.Extensions;

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
            _channel = CreateChannel(this.GetConnection());
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            this.ConfigureHandler(_channel, _subscriptionName, receiver);
        }

        public Task RegisterHandlerAsync(Func<ReceivedMessage, Task<MessageReceivedStatusEnum>> handler, Func<Exception, Task> errorHandler, Func<Task> onIdle = null, bool autoComplete = false)
        {
            _channel = CreateChannel(this.GetConnection());
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            this.ConfigureHandler(_channel, _subscriptionName, receiver);
            return Task.CompletedTask;
        }

        public new void Dispose()
        {   
            _channel?.Close();
            _conn?.Close();
            base.Dispose();
        }

        public new Task Close()
        {
            _channel?.Close();
            _conn?.Close();
            base.Dispose();
            return Task.CompletedTask;
        }

        
    }
}
