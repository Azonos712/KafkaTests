﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kafka.Consumer;

internal class Program
{
  static void Main(string[] args)
  {
    CreateHostBuilder(args).Build().Run();
    Console.ReadKey();
  }

  private static IHostBuilder CreateHostBuilder(string[] args) =>
    Host
      .CreateDefaultBuilder(args)
      .ConfigureServices((context, collection) =>
        collection.AddHostedService<KafkaConsumerService>());
}
