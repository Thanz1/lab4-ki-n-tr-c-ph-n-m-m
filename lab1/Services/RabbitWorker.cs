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

        // LỖI CỦA BẠN NẰM Ở ĐÂY: Phải có từ khóa 'override' và đúng tham số 'CancellationToken'
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Sửa lỗi tại dòng CreateConnectionAsync và CreateChannelAsync
            using var connection = await _factory.CreateConnectionAsync(cancellationToken: stoppingToken);
            using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await channel.QueueDeclareAsync(queue: "lab1_queue",
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null,
                                            cancellationToken: stoppingToken); // Thêm cho đồng bộ

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                Console.WriteLine($"\n[RabbitMQ] Đã nhận tin nhắn: {message}");

                await Task.CompletedTask;
            };

            await channel.BasicConsumeAsync(queue: "lab1_queue",
                                            autoAck: true,
                                            consumer: consumer,
                                            cancellationToken: stoppingToken); // Thêm cho đồng bộ

            // Giữ service chạy
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}