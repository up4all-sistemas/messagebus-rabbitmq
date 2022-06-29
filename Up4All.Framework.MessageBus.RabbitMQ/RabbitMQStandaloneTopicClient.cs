
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQStandaloneTopicClient : StandaloneRabbitMQClient, IMessageBusStandalonePublisher
    {
        private readonly string _topicName;

        public RabbitMQStandaloneTopicClient(ILogger<RabbitMQStandaloneTopicClient> logger, string connectionString, string topicName, int connectionAttemps = 8) : base(logger, connectionString, connectionAttemps)
        {
            _topicName = topicName;
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

            channel.BasicPublish(_topicName, routingKey, basicProps, msg.Body);
        }
    }
}
