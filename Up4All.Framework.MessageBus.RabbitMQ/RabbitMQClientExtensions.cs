using Polly;

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

using System;

using Up4All.Framework.MessageBus.Abstractions.Options;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public static class RabbitMQClientExtensions
    {
        public static IConnection GetConnection(this IRabbitMQClient client, MessageBusOptions opts)
        {
            var result = Policy
                .Handle<BrokerUnreachableException>()
                .WaitAndRetry(opts.ConnectionAttempts, retryAttempt => {
                    TimeSpan wait = TimeSpan.FromSeconds(Math.Pow(2, retryAttempt));                    
                    return wait;
                })
                .ExecuteAndCapture(() => {
                    return new ConnectionFactory() { Uri = new Uri(opts.ConnectionString) }.CreateConnection();
                });

            if (result.Outcome == OutcomeType.Successful)
                return result.Result;

            throw result.FinalException;
        }

        public static IModel CreateChannel(this IRabbitMQClient client, IConnection conn)
        {
            return conn.CreateModel();
        }
    }
}
