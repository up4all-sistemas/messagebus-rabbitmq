
using System.Collections.Generic;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Mocks;

namespace Up4All.Framework.MessageBus.RabbitMQ.Mocks
{
    public class RabbitMQTopicClientMocked : MessageBusTopicClientMock, IMessageBusPublisher, IRabbitMQClient
    {
        public RabbitMQTopicClientMocked() : base()
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
    }
}
