using System;
using System.IO;
using Confluent.Kafka;

public class TransformService
{
    private const string KafkaBroker = "localhost:9092";
    private const string RawDataTopic = "raw_data";
    private const string RawConfigTopic = "raw_config";
    private const string ProcessedTopic = "processed";

    static async Task Main()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "etl_transformer_group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Subscribe(new[] { RawDataTopic, RawConfigTopic });

        var producerConfig = new ProducerConfig { BootstrapServers = KafkaBroker };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        string data = null, config = null;

        Console.WriteLine("Transform Service started. Listening for messages...");

        while (true)
        {
            var result = consumer.Consume();
            if (result.Topic == RawDataTopic) data = result.Message.Value;
            if (result.Topic == RawConfigTopic) config = result.Message.Value;

            if (data != null && config != null)
            {
                string transformedData = data.ToUpper();  // Example transformation
                await producer.ProduceAsync(ProcessedTopic, new Message<Null, string> { Value = transformedData });

                Console.WriteLine($"Processed: {transformedData}");
                data = config = null;
            }
        }
    }
}
