using RabbitMQ.Client;

using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ.Extensions
{
    public static class StandaloneRabbitMQClientExtensions
    {
        public static IModel ConfigureHandler(this StandaloneRabbitMQClient client, string queue, QueueMessageReceiver receiver)
        {
            var channel = client.CreateChannel(client.GetConnection());
            channel.BasicQos(0, 1, false);
            channel.BasicConsume(queue: queue, autoAck: false, consumer: receiver);
            return channel;
        }
    }
}
