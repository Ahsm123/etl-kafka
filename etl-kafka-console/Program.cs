using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    private const string KafkaBroker = "localhost:9092";
    private const string RawDataTopic = "raw_data";

    public static async Task Main()
    {

        var producerConfig = new ProducerConfig { BootstrapServers = KafkaBroker };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        while (true)
        {
            Console.Write("Enter a message or type 'exit': ");
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

            int pipelineId;
            while (true)
            {
                Console.Write("Enter a pipeline ID (1 for Uppercase, 2 for Lowercase, 3 for Reverse): ");
                string? pipelineInput = Console.ReadLine();

                if (int.TryParse(pipelineInput, out pipelineId) && pipelineId > 0)
                {
                    break;
                }
                Console.WriteLine("Invalid input. Please enter a valid pipeline ID.");
            }

            var eventData = new
            {
                pipelineId = pipelineId,
                data = message
            };

            string jsonMessage = JsonSerializer.Serialize(eventData);


            await producer.ProduceAsync(RawDataTopic, new Message<Null, string> { Value = jsonMessage });

            Console.WriteLine("Message sent.");
        }
    }
}
