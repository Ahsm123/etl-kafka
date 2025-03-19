using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    private const string KafkaBroker = "localhost:9092";
    private const string RawDataTopic = "raw_data";
    private const string RawConfigTopic = "raw_config";

    static async Task Main()
    {
        Console.WriteLine("Kafka UI");

        var producerConfig = new ProducerConfig { BootstrapServers = KafkaBroker };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        while (true)
        {
            Console.Write("Enter a message (or type 'exit' to quit): ");
            string? message = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(message))
            {
                Console.WriteLine("Message cannot be empty.");
                continue;
            }

            if (message.Equals("exit", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Exiting...");
                break;
            }

            Console.WriteLine("Choose a transformation:");
            Console.WriteLine("1. Uppercase");
            Console.Write("Your choice: ");
            string? choice = Console.ReadLine();

            string transformation = "uppercase";


            await producer.ProduceAsync(RawDataTopic, new Message<Null, string> { Value = message });
            await producer.ProduceAsync(RawConfigTopic, new Message<Null, string> { Value = transformation });

            Console.WriteLine("Message sent.");
        }
    }
}
