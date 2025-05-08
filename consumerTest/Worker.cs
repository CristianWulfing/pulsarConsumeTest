using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Hosting;
using System.Text;

namespace consumerTest;

public sealed class Worker : BackgroundService
{
	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		await using var client = PulsarClient.Builder().Build();

		await using var consumer = client
			.NewConsumer(Schema.ByteArray)
			.Topic("persistent://test/key-shared/process")
			.SubscriptionName("ProgramStatusWorker")
			.SubscriptionType(SubscriptionType.KeyShared)
			.Create();

		var option = new ProcessingOptions()
		{
			MaxDegreeOfParallelism = 1,
			LinkTraces = true,
			EnsureOrderedAcknowledgment = false,
		};

		await consumer.Process(ProcessMessage, option, stoppingToken);
	}

	private ValueTask ProcessMessage(IMessage<byte[]> message, CancellationToken cancellationToken)
	{
		Console.WriteLine(Encoding.UTF8.GetString(message.Data));
		return ValueTask.CompletedTask;
	}
}
