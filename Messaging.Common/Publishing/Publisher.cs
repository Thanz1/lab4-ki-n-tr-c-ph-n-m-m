using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;

namespace Messaging.Common.Publishing
{
    public class Publisher
    {
        private readonly IModel _channel;

        public Publisher(IModel channel)
        {
            _channel = channel;
        }

        public void Publish<T>(string exchange, string routingKey, T message, string? correlationId = null)
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));
            var props = _channel.CreateBasicProperties();
            props.Persistent = true;
            props.CorrelationId = correlationId ?? Guid.NewGuid().ToString();

            _channel.BasicPublish(
                exchange: exchange,
                routingKey: routingKey,
                basicProperties: props,
                body: body
            );
        }
    }
}
