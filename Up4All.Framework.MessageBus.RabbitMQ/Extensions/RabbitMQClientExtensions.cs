using RabbitMQ.Client;

using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ.Extensions
{
    public static class RabbitMQClientExtensions
    {
        public static IModel ConfigureHandler(this RabbitMQClient client, MessageBusOptions opts, QueueMessageReceiver receiver)
        {
            var channel = client.CreateChannel(client.GetConnection());
            channel.BasicQos(0, 1, false);
            channel.BasicConsume(queue: opts.SubscriptionName, autoAck: false, consumer: receiver);
            return channel;
        }
    }
}
