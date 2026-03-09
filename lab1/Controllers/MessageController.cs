using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using System.Text;

[ApiController]
[Route("api/[controller]")]
public class MessageController : ControllerBase
{
    private readonly ConnectionFactory _factory;

    public MessageController(ConnectionFactory factory)
    {
        _factory = factory;
    }

    [HttpPost("send")]
    public async Task<IActionResult> SendMessage([FromBody] string message)
    {
        // Tạo kết nối và channel
        using var connection = await _factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        // Khai báo Queue (Tên hàng đợi)
        await channel.QueueDeclareAsync(queue: "lab1_queue",
                                        durable: false,
                                        exclusive: false,
                                        autoDelete: false);

        var body = Encoding.UTF8.GetBytes(message);

        // Gửi tin nhắn lên Queue
        await channel.BasicPublishAsync(exchange: string.Empty,
                                        routingKey: "lab1_queue",
                                        body: body);

        return Ok($"Đã gửi: {message}");
    }
}