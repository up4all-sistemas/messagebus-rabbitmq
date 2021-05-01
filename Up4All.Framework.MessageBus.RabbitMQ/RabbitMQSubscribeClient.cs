using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQSubscribeClient : MessageBusSubscribeClient, IRabbitMQClient, IDisposable
    {
        private IConnection _conn;
        private IModel _channel;

        public RabbitMQSubscribeClient(IOptions<MessageBusOptions> messageOptions) : base(messageOptions)
        {
        }

        public override void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            _conn = this.GetConnection(MessageBusOptions);
            _channel = this.CreateChannel(_conn);
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: MessageBusOptions.SubscriptionName, autoAck: false, consumer: receiver);            
        }

        public void Dispose()
        {
            _channel?.Close();
            _conn?.Close();
        }

        public override Task Close()
        {
            _channel?.Close();
            _conn?.Close();

            return Task.CompletedTask;
        }
    }
}
