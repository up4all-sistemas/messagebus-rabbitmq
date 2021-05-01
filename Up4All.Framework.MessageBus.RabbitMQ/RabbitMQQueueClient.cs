using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQQueueClient : MessageBusQueueClient, IRabbitMQClient, IDisposable
    {
        private IConnection _conn;
        private IModel _channel;

        public RabbitMQQueueClient(IOptions<MessageBusOptions> messageOptions) : base(messageOptions)
        {            
        }

        public override void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            _conn = this.GetConnection(MessageBusOptions);
            _channel = this.CreateChannel(_conn);
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: MessageBusOptions.QueueName, autoAck: false, consumer: receiver);
        }

        public override Task Send(MessageBusMessage message)
        {
            using (var conn = this.GetConnection(MessageBusOptions))
            {
                using (var channel = this.CreateChannel(conn))
                {                    
                    Send(message, channel);
                }
            }

            return Task.CompletedTask;
        }

        public override Task Send(IEnumerable<MessageBusMessage> messages)
        {
            using (var conn = this.GetConnection(MessageBusOptions))
            {
                using (var channel = this.CreateChannel(conn))
                {
                    foreach (var message in messages)
                        Send(message, channel);
                }
            }

            return Task.CompletedTask;
        }

        private void Send(MessageBusMessage msg, IModel channel)
        {
            IBasicProperties basicProps = channel.CreateBasicProperties();

            if (msg.UserProperties.Any())
                basicProps.Headers = msg.UserProperties;

            channel.BasicPublish(exchange: "", routingKey: MessageBusOptions.QueueName, basicProperties: basicProps, body: msg.Body);
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
