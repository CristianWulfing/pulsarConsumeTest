using DotPulsar;
using DotPulsar.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

namespace producerTest;

public sealed class Worker : BackgroundService
{
	private readonly ILogger _logger;

	public Worker(ILogger<Worker> logger) => _logger = logger;

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await using var client = PulsarClient.Builder().Build();

		await using var producer = client
			.NewProducer(Schema.ByteArray)
			.Topic("persistent://test/key-shared/process")
			.Create();

		var delay = TimeSpan.FromSeconds(0.3);

		_logger.LogInformation($"Will start sending messages every {delay.TotalSeconds} seconds with 'Send'");

		while (!stoppingToken.IsCancellationRequested)
		{

			string messageKey = "key1";
			var message = DateTime.UtcNow.ToLongTimeString();
			var messageBytes = Encoding.UTF8.GetBytes(message);
			var keyBytes = Encoding.UTF8.GetBytes(messageKey);
			var pulsarMessage = producer.NewMessage().KeyBytes(keyBytes);

			var messageId = await pulsarMessage.Send(messageBytes, stoppingToken);

			_logger.LogInformation($"Sent message with content: '{message}' and got message id: '{messageId}'");
			await Task.Delay(delay, stoppingToken);
		}
	}
}