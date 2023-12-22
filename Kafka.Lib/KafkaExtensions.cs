using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Lib;
public static class KafkaExtensions
{
  public static async Task CreateTopic(string _topicName, int _numPartitions)
  {
    using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();

    try
    {
      var topicSpec = new TopicSpecification
      {
        Name = _topicName,
        NumPartitions = _numPartitions
      };

      await adminClient.CreateTopicsAsync(new[] { topicSpec });
      Console.WriteLine($"Topic '{_topicName}' created with {_numPartitions} partitions.");
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error creating topic: {ex.Message}");
    }
  }

  public static async Task DeleteTopic(string _topicName)
  {
    using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();

    try
    {
      // Попытка получить метаданные топика
      var metadata = adminClient.GetMetadata(_topicName, TimeSpan.FromSeconds(5));
      var existingTopic = metadata.Topics.Find(topic => topic.Topic == _topicName);

      if (existingTopic != null)
      {
        // Топик существует, пытаемся удалить его
        await adminClient.DeleteTopicsAsync(new[] { _topicName });
        Console.WriteLine($"Topic '{_topicName}' deleted.");
      }
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Error deleting topic: {ex.Message}");
    }
  }
}
