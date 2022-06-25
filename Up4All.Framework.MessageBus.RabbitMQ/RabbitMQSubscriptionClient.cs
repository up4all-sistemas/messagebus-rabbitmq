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
using Up4All.Framework.MessageBus.RabbitMQ.Extensions;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQSubscriptionClient : RabbitMQClient, IMessageBusConsumer, IDisposable
    {
        private IModel _channel;

        public RabbitMQSubscriptionClient(ILogger<RabbitMQSubscriptionClient> logger, IOptions<MessageBusOptions> messageOptions) : base(logger, messageOptions)
        {
        }

        public void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel = this.ConfigureHandler(MessageBusOptions, receiver);
        }

        public Task RegisterHandlerAsync(Func<ReceivedMessage, Task<MessageReceivedStatusEnum>> handler, Func<Exception, Task> errorHandler, Func<Task> onIdle = null, bool autoComplete = false)
        {            
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel = this.ConfigureHandler(MessageBusOptions, receiver);
            return Task.CompletedTask;
        }

        public new void Dispose()
        {
            _channel?.Close();
            base.Close();
        }

        public new Task Close()
        {
            _channel?.Close();
            base.Close();

            return Task.CompletedTask;
        }

    }
}
