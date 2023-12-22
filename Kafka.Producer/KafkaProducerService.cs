using Confluent.Kafka;
using Kafka.Lib;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Producer;

internal class KafkaProducerService : IHostedService
{
  private readonly ILogger<KafkaProducerService> p_logger;
  private readonly IProducer<Null, string> p_producer;

  public KafkaProducerService(ILogger<KafkaProducerService> _logger)
  {
    p_logger = _logger;
    var config = new ProducerConfig
    {
      BootstrapServers = "localhost:9092"
    };
    p_producer = new ProducerBuilder<Null, string>(config).Build();

    //KafkaExtensions.DeleteTopic("main-topic").Wait();
    //KafkaExtensions.CreateTopic("main-topic", 1).Wait();
  }

  public async Task StartAsync(CancellationToken _cancellationToken)
  {
    int i = 0;
    while (true)
    {
      var startTime = DateTime.Now;
      var value = $"{startTime.ToShortTimeString()} - New msg #{i} - {startTime.Ticks}";
      p_logger.LogInformation($"Sending >> {value}");
      await p_producer.ProduceAsync(
          "main-topic",
          new Message<Null, string> { Value = value },
          _cancellationToken);
      i++;

      p_logger.LogInformation($"Press any key to produce msg or `Esc` to stop programm");
      var key = Console.ReadKey();
      if (key.Key == ConsoleKey.Escape)
        break;
    }
  }

  public Task StopAsync(CancellationToken _cancellationToken)
  {
    p_producer?.Dispose();
    p_logger.LogInformation($"{nameof(KafkaProducerService)} stopped");
    return Task.CompletedTask;
  }
}
