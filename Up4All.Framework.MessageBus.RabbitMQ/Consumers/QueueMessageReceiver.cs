using RabbitMQ.Client;

using System;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Messages;

namespace Up4All.Framework.MessageBus.RabbitMQ.Consumers
{
    public class QueueMessageReceiver : DefaultBasicConsumer
    {
        private readonly IModel _channel;
        private readonly Func<ReceivedMessage, MessageReceivedStatusEnum> _handler;
        private readonly Action<Exception> _errorHandler;

        public QueueMessageReceiver(IModel channel, Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler)
        {
            _channel = channel;
            _handler = handler;
            _errorHandler = errorHandler;
        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            try
            {
                var message = new ReceivedMessage();
                message.AddBody(body.ToArray());

                if(properties.Headers != null)
                    foreach (var prop in properties.Headers)
                        message.UserProperties.Add(prop.Key, prop.Value);

                var response = _handler(message);

                if (response == MessageReceivedStatusEnum.Deadletter)
                    _channel.BasicReject(deliveryTag, false);

                _channel.BasicAck(deliveryTag, false);
            }
            catch (Exception ex)
            {
                _channel.BasicNack(deliveryTag, false, false);
                _errorHandler(ex);
            }
        }
    }
}
