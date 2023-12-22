using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Consumer;

internal class KafkaConsumerService : IHostedService
{
  private readonly ILogger<KafkaConsumerService> p_logger;
  private readonly IConsumer<Ignore, string> p_consumer;

  public KafkaConsumerService(ILogger<KafkaConsumerService> _logger)
  {
    p_logger = _logger;
    var config = new ConsumerConfig
    {
      BootstrapServers = "localhost:9092",
      GroupId = "work-group",
      AutoOffsetReset = AutoOffsetReset.Earliest
    };
    p_consumer = new ConsumerBuilder<Ignore, string>(config).Build();
  }

  public Task StartAsync(CancellationToken _cancellationToken)
  {
    p_consumer.Subscribe("inspector-topic");
    while (!_cancellationToken.IsCancellationRequested)
    {
      var consumeResult = p_consumer.Consume(_cancellationToken);

      var receivedTime = DateTime.Now;

      var messageParts = consumeResult.Message.Value.Split('-');
      var messageTime = DateTime.Parse(messageParts[0].Trim());
      var messageContent = messageParts[1].Trim();
      var messageTimestamp = long.Parse(messageParts[2].Trim());

      var elapsedTimeTicks = receivedTime.Ticks - messageTimestamp;
      var elapsedTimeMilliseconds = elapsedTimeTicks / TimeSpan.TicksPerMillisecond;

      p_logger.LogInformation($"Received >> {receivedTime.ToShortTimeString()} - {messageContent} (pass {elapsedTimeMilliseconds} ms or  {elapsedTimeTicks} ticks)");
    }
    return Task.CompletedTask;
  }

  public Task StopAsync(CancellationToken cancellationToken)
  {
    p_consumer?.Dispose();
    p_logger.LogInformation($"{nameof(KafkaConsumerService)} stopped");
    return Task.CompletedTask;
  }
}
