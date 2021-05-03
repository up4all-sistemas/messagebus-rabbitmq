using Up4All.Framework.MessageBus.Abstractions;
using Up4All.Framework.MessageBus.Abstractions.Configurations;
using Up4All.Framework.MessageBus.Abstractions.Messages;
using Up4All.Framework.MessageBus.Abstractions.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Xunit;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;

namespace Up4All.Framework.MessageBus.RabbitMQ.Tests
{
    public class RabbitMQTestsImpl
    {
        private readonly IServiceProvider _provider;
        private readonly IConfiguration _configuration;

        public RabbitMQTestsImpl()
        {
            _configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

            var services = new ServiceCollection();

            services.AddMessageBusQueueClient<RabbitMQQueueClient>(_configuration);
            services.AddMessageBusTopicClient<RabbitMQTopicClient>(_configuration);
            services.AddMessageBusSubscribeClient<RabbitMQSubscribeClient>(_configuration);

            _provider = services.BuildServiceProvider();
        }

        [Fact]
        public async void QueueSendMessage()
        {
            var client = _provider.GetRequiredService<IMessageBusQueueClient>();

            var msg = new MessageBusMessage()
            {                
            };
            msg.AddBody(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { teste = "teste", numero = 10 }));
            msg.UserProperties.Add("proptst", "tst");

            await client.Send(msg);

            Assert.True(true);
        }

        [Fact]
        public void QueueReceiveMessage()
        {
            var client = _provider.GetRequiredService<IMessageBusQueueClient>();

            client.RegisterHandler((msg) =>
            {
                Assert.True(msg != null);
                return Abstractions.Enums.MessageReceivedStatusEnum.Completed;
            }, (ex) => Debug.Print(ex.Message));


            Thread.Sleep(5000);
        }

        [Fact]
        public async void TopicSendMessage()
        {
            var client = _provider.GetRequiredService<IMessageBusPublisher>();

            var msg = new MessageBusMessage();
            msg.AddBody(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { teste = "teste", numero = 10 }));
            msg.UserProperties.Add("proptst", "tst");

            await client.Send(msg);

            Assert.True(true);
        }

        [Fact]
        public void SubscriveReceiveMessage()
        {
            var client = _provider.GetRequiredService<IMessageBusConsumer>();

            client.RegisterHandler((msg) =>
            {
                Assert.True(msg != null);
                return Abstractions.Enums.MessageReceivedStatusEnum.Completed;
            }, (ex) => Debug.Print(ex.Message));

            Thread.Sleep(5000);
        }
    }
}
