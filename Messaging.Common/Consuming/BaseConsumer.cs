using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Messaging.Common.Consuming
{
    public abstract class BaseConsumer<T>
    {
        private readonly IModel _channel;
        private readonly string _queue;

        protected BaseConsumer(IModel channel, string queue)
        {
            _channel = channel;
            _queue = queue;
        }

        public void Consume()
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var data = JsonSerializer.Deserialize<T>(message);

                if (data != null)
                {
                    try
                    {
                        await ProcessMessage(data, ea.BasicProperties.CorrelationId);
                        _channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception)
                    {
                        _channel.BasicNack(ea.DeliveryTag, false, true);
                    }
                }
            };

            _channel.BasicConsume(queue: _queue, autoAck: false, consumer: consumer);
        }

        protected abstract Task ProcessMessage(T message, string correlationId);
    }
}
