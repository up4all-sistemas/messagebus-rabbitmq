using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQTopicClient : RabbitMQClient, IMessageBusPublisher
    {
        public RabbitMQTopicClient(ILogger<RabbitMQTopicClient> logger, IOptions<MessageBusOptions> messageOptions) : base(logger, messageOptions)
        {
        }

        public Task Send(MessageBusMessage message)
        {
            using (var channel = CreateChannel(GetConnection()))
            {
                Send(message, channel);
            }

            return Task.CompletedTask;
        }

        public Task Send(IEnumerable<MessageBusMessage> messages)
        {
            using (var channel = CreateChannel(GetConnection()))
            {
                foreach (var message in messages)
                    Send(message, channel);
            }

            return Task.CompletedTask;
        }

        private void Send(MessageBusMessage msg, IModel channel)
        {
            var basicProps = channel.CreateBasicProperties();

            if (msg.UserProperties.Any())
                basicProps.Headers = msg.UserProperties;

            var routingKey = "";

            if (msg.UserProperties.ContainsKey("routing-key"))
                routingKey = msg.UserProperties["routing-key"].ToString();

            channel.BasicPublish(MessageBusOptions.TopicName, routingKey, basicProps, msg.Body);
        }
    }
}
