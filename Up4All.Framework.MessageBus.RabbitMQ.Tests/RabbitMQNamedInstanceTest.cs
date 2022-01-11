using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Up4All.Framework.MessageBus.Abstractions.Configurations;
using Up4All.Framework.MessageBus.Abstractions.Factories;
using Up4All.Framework.MessageBus.Abstractions.Interfaces;
using Up4All.Framework.MessageBus.Abstractions.Messages;

using Xunit;

namespace Up4All.Framework.MessageBus.RabbitMQ.Tests
{
    public class RabbitMQNamedInstanceTest
    {
        private readonly IServiceProvider _provider;
        private readonly IConfiguration _configuration;

        public RabbitMQNamedInstanceTest()
        {
            _configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

            var services = new ServiceCollection();

            services.AddMessageBusNamedQueueClient("queue1", provider =>
            {
                return new RabbitMQStandaloneQueueClient(_configuration.GetValue<string>("MessageBusOptions:ConnectionString")
                    ,_configuration.GetValue<string>("MessageBusOptions:QueueName"));
            });

            _provider = services.BuildServiceProvider();
        }

        [Fact]
        public async void QueueSendMessage()
        {
            var factory = _provider.GetRequiredService<MessageBusFactory>();
            var client = factory.GetQueueClient("queue1");

            var msg = new MessageBusMessage()
            {
            };
            msg.AddBody(System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { teste = "teste", numero = 10 }));
            msg.UserProperties.Add("proptst", "tst");

            await client.Send(msg);

            Assert.True(true);
        }
    }
}
