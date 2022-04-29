using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQSubscriptionClient : RabbitMQClient, IMessageBusConsumer, IDisposable
    {
        private IConnection _conn;
        private IModel _channel;

        public RabbitMQSubscriptionClient(ILogger<RabbitMQSubscriptionClient> logger, IOptions<MessageBusOptions> messageOptions) : base(logger, messageOptions)
        {
        }

        public void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            _conn = GetConnection();
            _channel = CreateChannel(_conn);
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: MessageBusOptions.SubscriptionName, autoAck: false, consumer: receiver);
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
