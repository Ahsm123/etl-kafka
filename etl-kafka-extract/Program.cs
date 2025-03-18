using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);



builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton(new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = "127.0.0.1:9092" }).Build());

var app = builder.Build();

app.MapPost("/extract", async ([FromBody] ExtractMessage message, IProducer<Null, string> producer) =>
{
    await producer.ProduceAsync("raw_data", new Message<Null, string> { Value = message.data });
    await producer.ProduceAsync("raw_data", new Message<Null, string> { Value = message.config });
    return Results.Ok("Data sent to Kafka");
});

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

record ExtractMessage(string data, string config);
