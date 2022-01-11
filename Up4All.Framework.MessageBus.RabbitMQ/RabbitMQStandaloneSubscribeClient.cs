using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Enums;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Up4All.Framework.MessageBus.RabbitMQ.Consumers;

namespace Up4All.Framework.MessageBus.RabbitMQ
{
    public class RabbitMQStandaloneSubscribeClient : MessageBusStandaloneSubscribeClient, IRabbitMQClient, IDisposable
    {
        private IConnection _conn;
        private IModel _channel;
        private readonly string _connectionString;
        private readonly string _topicName;
        private readonly string _subscriptionName;

        public RabbitMQStandaloneSubscribeClient(string connectionString, string topicName, string subscriptionName) : base(connectionString, topicName, subscriptionName)
        {
            this._connectionString = connectionString;
            this._topicName = topicName;
            this._subscriptionName = subscriptionName;
        }

        public override void RegisterHandler(Func<ReceivedMessage, MessageReceivedStatusEnum> handler, Action<Exception> errorHandler, Action onIdle = null, bool autoComplete = false)
        {            
            _channel = this.CreateChannel(this.GetConnection());
            _channel.BasicQos(0, 1, false);
            var receiver = new QueueMessageReceiver(_channel, handler, errorHandler);
            _channel.BasicConsume(queue: _subscriptionName, autoAck: false, consumer: receiver);            
        }

        public void Dispose()
        {
            _channel?.Close();
            _conn?.Close();
        }

        public override Task Close()
        {
            _channel?.Close();
            _conn?.Close();

            return Task.CompletedTask;
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
