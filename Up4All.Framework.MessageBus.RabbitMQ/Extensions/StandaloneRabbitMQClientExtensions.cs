using RabbitMQ.Client;

using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ.Extensions
{
    public static class StandaloneRabbitMQClientExtensions
    {
        public static IModel CreateChannel(this StandaloneRabbitMQClient client)
        {
            return client.CreateChannel(client.GetConnection());
        }

        public static void ConfigureHandler(this StandaloneRabbitMQClient client, IModel channel, string queue, QueueMessageReceiver receiver)
        {            
            channel.BasicQos(0, 1, false);
            channel.BasicConsume(queue: queue, autoAck: false, consumer: receiver);            
        }
    }
}
