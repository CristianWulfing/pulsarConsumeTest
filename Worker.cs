using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System.Text;

namespace pulsarConsumeTest;

public sealed class Worker : BackgroundService
{
	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await using var client = PulsarClient.Builder().Build();

		await using var consumer = client
			.NewConsumer(Schema.String)
			.SubscriptionName("SimpleWorker")
			.Topic("persistent://simple-topic")
			.Create();

		await consumer.Process(ProcessMessage, stoppingToken);
	}

	private ValueTask ProcessMessage(IMessage<string> message, CancellationToken cancellationToken)
	{
		Console.WriteLine(Encoding.UTF8.GetString(message.Data));
		return ValueTask.CompletedTask;
	}
}
