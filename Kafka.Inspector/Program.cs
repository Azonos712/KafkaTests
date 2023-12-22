using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kafka.Inspector;

internal class Program
{
  static void Main(string[] args)
  {
    CreateHostBuilder(args).Build().Run();
    Console.ReadKey();
  }

  private static IHostBuilder CreateHostBuilder(string[] _args) =>
    Host
      .CreateDefaultBuilder(_args)
      .ConfigureServices((_context, _collection) =>
        _collection.AddHostedService<KafkaInspectorService>());
}