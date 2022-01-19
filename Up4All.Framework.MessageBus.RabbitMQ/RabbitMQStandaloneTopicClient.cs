
using RabbitMQ.Client;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Messages;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQStandaloneTopicClient : MessageBusStandaloneTopicClient, IRabbitMQClient
    {
        private IConnection _conn;

        private readonly string _connectionString;
        private readonly string _topicName;

        public RabbitMQStandaloneTopicClient(string connectionString, string topicName) : base(connectionString, topicName)
        {
            this._connectionString = connectionString;
            this._topicName = topicName;
        }

        public override Task Send(MessageBusMessage message)
        {
            using (var conn = this.GetConnection())
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
            using (var conn = this.GetConnection())
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

            channel.BasicPublish(_topicName, routingKey, basicProps, msg.Body);
        }

        private IConnection GetConnection()
        {
            if (_conn != null)
                return _conn;

            _conn = new ConnectionFactory() { Uri = new Uri(_connectionString) }.CreateConnection();
            return _conn;
        }
    }
}
