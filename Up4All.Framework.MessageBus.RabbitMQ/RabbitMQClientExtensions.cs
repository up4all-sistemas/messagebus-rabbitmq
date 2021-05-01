using RabbitMQ.Client;

using System;

using Up4All.Framework.MessageBus.Abstractions.Options;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public static class RabbitMQClientExtensions
    {
        public static IConnection GetConnection(this IRabbitMQClient client, MessageBusOptions opts)
        {
            return new ConnectionFactory() { Uri = new Uri(opts.ConnectionString) }.CreateConnection();
        }

        public static IModel CreateChannel(this IRabbitMQClient client, IConnection conn)
        {
            return conn.CreateModel();
        }
    }
}
