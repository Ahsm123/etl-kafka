using Confluent.Kafka;

namespace etl_kafka_load;

public class LoadService
{
    private const string KafkaBroker = "127.0.0.1:9092";
    private const string ProcessedTopic = "processed";
    private const string OutputDirectory = "./output";

    static void Main()
    {
        Directory.CreateDirectory(OutputDirectory);
        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "etl_loader_group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(ProcessedTopic);
        Console.WriteLine("Load Service started. Listening for messages...");

        while (true)
        {
            var result = consumer.Consume();
            string filePath = Path.Combine(OutputDirectory, $"output_{DateTime.UtcNow:yyyyMMddHHmmss}.txt");
            File.WriteAllText(filePath, result.Message.Value);
            Console.WriteLine($"Saved output to {filePath}");
        }
    }
}
