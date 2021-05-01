using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQTopicClient : MessageBusTopicClient, IRabbitMQClient
    {
        public RabbitMQTopicClient(IOptions<MessageBusOptions> messageOptions) : base(messageOptions)
        {
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
