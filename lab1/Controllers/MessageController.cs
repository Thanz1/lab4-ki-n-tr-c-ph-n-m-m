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

    [HttpPost("direct")]
    public async Task<IActionResult> SendDirect([FromBody] string message)
    {
        using var connection = await _factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var body = Encoding.UTF8.GetBytes(message);
        await channel.BasicPublishAsync(exchange: "TestExchange", routingKey: "test", body: body);

        return Ok($"Sent to TestExchange with key 'test': {message}");
    }

    [HttpPost("topic/{key}")]
    public async Task<IActionResult> SendTopic(string key, [FromBody] string message)
    {
        using var connection = await _factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var body = Encoding.UTF8.GetBytes(message);
        await channel.BasicPublishAsync(exchange: "PatternExchange", routingKey: key, body: body);

        return Ok($"Sent to PatternExchange with key '{key}': {message}");
    }

    [HttpPost("headers/{app}")]
    public async Task<IActionResult> SendHeaders(string app, [FromBody] string message)
    {
        using var connection = await _factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var body = Encoding.UTF8.GetBytes(message);
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { { "app", app } }
        };

        await channel.BasicPublishAsync(exchange: "HeadersExchange", routingKey: string.Empty, mandatory: false, basicProperties: properties, body: body);

        return Ok($"Sent to HeadersExchange with header 'app={app}': {message}");
    }

    [HttpPost("fanout")]
    public async Task<IActionResult> SendFanout([FromBody] string message)
    {
        using var connection = await _factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var body = Encoding.UTF8.GetBytes(message);
        await channel.BasicPublishAsync(exchange: "FanoutExchange", routingKey: string.Empty, body: body);

        return Ok($"Sent to FanoutExchange: {message}");
    }
}