using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Confluent.Kafka;

public class TransformService
{
    private const string KafkaBroker = "localhost:9092";
    private const string RawDataTopic = "raw_data";
    private const string ProcessedTopic = "processed";

    public static async Task Main()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "etl_transformer_group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Subscribe(RawDataTopic);

        var producerConfig = new ProducerConfig { BootstrapServers = KafkaBroker };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        Console.WriteLine("Transform Service started. Listening for messages...");

        while (true)
        {
            var result = consumer.Consume();
            var inputMessage = result.Message.Value;

            Console.WriteLine($"DEBUG: Received JSON -> {inputMessage}");

            try
            {
                var eventData = JsonSerializer.Deserialize<EventMessage>(inputMessage, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                if (eventData == null || string.IsNullOrEmpty(eventData.Data))
                {
                    Console.WriteLine("Invalid message format: Deserialized object is null or missing fields.");
                    continue;
                }

                string transformedData = ApplyTransformation(eventData.Data, eventData.PipelineId);

                await producer.ProduceAsync(ProcessedTopic, new Message<Null, string> { Value = transformedData });
                Console.WriteLine($"Processed: {transformedData} (Pipeline: {eventData.PipelineId})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
            }
        }
    }

    static string ApplyTransformation(string input, int pipelineId)
    {
        Console.WriteLine($"DEBUG: Applying transformation for PipelineId {pipelineId} to input '{input}'");

        return pipelineId switch
        {
            1 => input.ToUpper(),
            2 => input.ToLower(),
            3 => new string(input.Reverse().ToArray()),
            _ => input
        };
    }
}

public class EventMessage
{
    [JsonPropertyName("pipelineId")]
    public int PipelineId { get; set; }

    [JsonPropertyName("data")]
    public string Data { get; set; }
}
