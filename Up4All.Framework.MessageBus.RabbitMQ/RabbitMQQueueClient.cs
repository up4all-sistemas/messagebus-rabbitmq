﻿using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQQueueClient : RabbitMQClient, IMessageBusQueueClient, IDisposable
    {
                
        private IModel _channel;

        public RabbitMQQueueClient(IOptions<MessageBusOptions> messageOptions, ILogger<RabbitMQQueueClient> logger) : base(logger, messageOptions)
        {        
        }

        public void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {
            _channel = this.CreateChannel(GetConnection());
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: MessageBusOptions.QueueName, autoAck: false, consumer: receiver);
        }

        public Task Send(MessageBusMessage message)
        {
            using (var conn = GetConnection())
            {
                using (var channel = this.CreateChannel(conn))
                {
                    Send(message, channel);
                }
            }

            return Task.CompletedTask;
        }

        public Task Send(IEnumerable<MessageBusMessage> messages)
        {
            using (var conn = GetConnection())
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
