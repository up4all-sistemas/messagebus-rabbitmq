﻿
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Mocks;

namespace Up4All.Framework.MessageBus.RabbitMQ.Mocks
{
    public class RabbitMQQueueClientMocked : MessageBusQueueClientMock, IMessageBusQueueClient, IRabbitMQClient, IDisposable
    {
        public RabbitMQQueueClientMocked() : base()
        {
        }

        public override void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {

        }

        public override Task Send(MessageBusMessage message)
        {


            return Task.CompletedTask;
        }

        public override Task Send(IEnumerable<MessageBusMessage> messages)
        {

            return Task.CompletedTask;
        }

        public void Dispose()
        {

        }

        public override Task Close()
        {
            return Task.CompletedTask;
        }
    }
}
