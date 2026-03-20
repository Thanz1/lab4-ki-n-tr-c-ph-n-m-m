using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace lab1.Services
{
    // RabbitWorker kế thừa từ BackgroundService
    public class RabbitWorker : BackgroundService
    {
        private readonly ConnectionFactory _factory;

        public RabbitWorker(ConnectionFactory factory)
        {
            _factory = factory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                using var connection = await _factory.CreateConnectionAsync(cancellationToken: stoppingToken);
                using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

                // 1. Declare Direct Exchange
                await channel.ExchangeDeclareAsync(exchange: "TestExchange", type: ExchangeType.Direct, durable: true);
                await channel.QueueDeclareAsync(queue: "TestQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "TestQueue", exchange: "TestExchange", routingKey: "test");

                // 2. Declare Topic Exchange
                await channel.ExchangeDeclareAsync(exchange: "PatternExchange", type: ExchangeType.Topic, durable: true);
                await channel.QueueDeclareAsync(queue: "ErrorQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "ErrorQueue", exchange: "PatternExchange", routingKey: "log.error");
                await channel.QueueDeclareAsync(queue: "AllLogsQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "AllLogsQueue", exchange: "PatternExchange", routingKey: "log.*");

                // 3. Declare Headers Exchange
                await channel.ExchangeDeclareAsync(exchange: "HeadersExchange", type: ExchangeType.Headers, durable: true);
                await channel.QueueDeclareAsync(queue: "MobileQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "MobileQueue", exchange: "HeadersExchange", routingKey: string.Empty,
                    arguments: new Dictionary<string, object?> { { "app", "mobile" }, { "x-match", "all" } });
                await channel.QueueDeclareAsync(queue: "WebQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "WebQueue", exchange: "HeadersExchange", routingKey: string.Empty,
                    arguments: new Dictionary<string, object?> { { "app", "web" }, { "x-match", "all" } });

                // 4. Declare Fanout Exchange
                await channel.ExchangeDeclareAsync(exchange: "FanoutExchange", type: ExchangeType.Fanout, durable: true);
                await channel.QueueDeclareAsync(queue: "EmailQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "EmailQueue", exchange: "FanoutExchange", routingKey: string.Empty);
                await channel.QueueDeclareAsync(queue: "SMSQueue", durable: true, exclusive: false, autoDelete: false);
                await channel.QueueBindAsync(queue: "SMSQueue", exchange: "FanoutExchange", routingKey: string.Empty);

                var queues = new[] { "TestQueue", "ErrorQueue", "AllLogsQueue", "MobileQueue", "WebQueue", "EmailQueue", "SMSQueue" };

                foreach (var queueName in queues)
                {
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += async (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine($"\n[RabbitMQ] [{queueName}] Received: {message}");
                        await Task.CompletedTask;
                    };

                    await channel.BasicConsumeAsync(queue: queueName,
                                                    autoAck: true,
                                                    consumer: consumer,
                                                    cancellationToken: stoppingToken);
                }

                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[RabbitMQ ERROR] {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"[Inner Error] {ex.InnerException.Message}");
                }
            }
        }
    }
}