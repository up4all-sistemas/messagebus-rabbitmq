using RabbitMQ.Client;

using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.BaseClients;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ.Extensions
{
    public static class RabbitMQClientExtensions
    {
        public static void ConfigureHandler(this RabbitMQClient client, IModel channel, MessageBusOptions opts, QueueMessageReceiver receiver)
        {            
            channel.BasicQos(0, 1, false);
            channel.BasicConsume(queue: opts.SubscriptionName, autoAck: false, consumer: receiver);            
        }
    }
}
