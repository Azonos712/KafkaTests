using Confluent.Kafka;
using Kafka.Lib;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Inspector;
internal class KafkaInspectorService : IHostedService
{
  private readonly ILogger<KafkaInspectorService> p_logger;
  private readonly IConsumer<Ignore, string> p_consumer;
  private readonly IProducer<Null, string> p_producer;

  public KafkaInspectorService(ILogger<KafkaInspectorService> _logger)
  {
    p_logger = _logger;

    var consumerConfig = new ConsumerConfig
    {
      BootstrapServers = "localhost:9092",
      GroupId = "inspector-group",
      AutoOffsetReset = AutoOffsetReset.Earliest
    };
    p_consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

    var producerConfig = new ProducerConfig
    {
      BootstrapServers = "localhost:9092"
    };
    p_producer = new ProducerBuilder<Null, string>(producerConfig).Build();

    //KafkaExtensions.DeleteTopic("inspector-topic").Wait();
    //KafkaExtensions.CreateTopic("inspector-topic", 3).Wait();
  }

  public async Task StartAsync(CancellationToken _cancellationToken)
  {
    p_consumer.Subscribe("main-topic");

    while (!_cancellationToken.IsCancellationRequested)
    {
      var consumeResult = p_consumer.Consume(_cancellationToken);

      p_logger.LogInformation($"Inspect >> {consumeResult.Message.Value}");

      await p_producer.ProduceAsync(
          "inspector-topic",
          new Message<Null, string> { Value = consumeResult.Message.Value },
          _cancellationToken);

    }
  }

  public Task StopAsync(CancellationToken _cancellationToken)
  {
    p_consumer?.Dispose();
    p_producer?.Dispose();
    p_logger.LogInformation($"{nameof(KafkaInspectorService)} stopped");
    return Task.CompletedTask;
  }
}
