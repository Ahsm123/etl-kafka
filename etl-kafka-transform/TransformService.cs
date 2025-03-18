using Confluent.Kafka;

namespace etl_kafka_transform;

public class TransformService
{
    private const string KafkaBroker = "127.0.0.1:9092";
    private const string RawDataTopic = "raw_data";
    private const string RawConfigTopic = "raw_config";
    private const string ProcessedTopic = "processed";

    static async Task Main()
    {
        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = KafkaBroker,
            GroupId = "etl_transformer_group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(new[] { RawDataTopic, RawConfigTopic });

        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = KafkaBroker }).Build();

        string data = null, config = null;
        while (true)
        {
            var result = consumer.Consume();
            if (result.Topic == RawDataTopic) data = result.Message.Value;
            if (result.Topic == RawConfigTopic) config = result.Message.Value;

            if (data != null && config != null)
            {
                string transformedData = data.ToUpper();
                await producer.ProduceAsync(ProcessedTopic, new Message<Null, string> { Value = transformedData });
                Console.WriteLine($"Processed: {transformedData}");
                data = config = null;
            }
        }
    }
}
