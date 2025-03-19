using System;
using System.Threading.Tasks;
using Confluent.Kafka;



public class LoadService
{
    private const string KafkaBroker = "localhost:9092";
    private const string ProcessedTopic = "processed";
    private const string OutputDirectory = "./output";

    static void Main()
    {
        Directory.CreateDirectory(OutputDirectory);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "etl_loader_group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
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

