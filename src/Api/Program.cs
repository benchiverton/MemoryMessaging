using MemoryMessaging.Api.Messaging;
using MemoryMessaging.Api.Messaging.Options;
using MemoryMessaging.Api.Metrics;
using MemoryMessaging.Api.Transport;
using MemoryMessaging.Api.Transport.RingBufferSharedMemory;
using MemoryMessaging.Api.Transport.SynchronizedSharedMemory;
using MemoryMessaging.Api.Transport.Tcp;
using Serilog;
using Serilog.Formatting.Compact;

var builder = WebApplication.CreateBuilder(args);

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(new RenderedCompactJsonFormatter())
    .CreateLogger();

builder.Host.UseSerilog();

builder.Services.Configure<MessagingOptions>(
    builder.Configuration.GetSection(MessagingOptions.SectionName));

builder.Services.Configure<MetricsOptions>(
    builder.Configuration.GetSection(MetricsOptions.SectionName));

var messagingOptions = builder.Configuration
    .GetSection(MessagingOptions.SectionName)
    .Get<MessagingOptions>() ?? new MessagingOptions();

builder.Services.AddSingleton<IMessageTransport, TcpMessageTransport>();
builder.Services.AddSingleton<IMessageTransport, SynchronizedSharedMemoryMessageTransport>();
builder.Services.AddSingleton<IMessageTransport, RingBufferSharedMemoryMessageTransport>();
builder.Services.AddSingleton<IMessageTransportRegistry, MessageTransportRegistry>();
builder.Services.AddSingleton<IRequestHandler, BounceRequestHandler>();
builder.Services.AddSingleton<LatencyMetrics>();
builder.Services.AddSingleton<IRttMetrics>(services => services.GetRequiredService<LatencyMetrics>());

builder.Services.AddHostedService<MessageConsumerService>();
builder.Services.AddHostedService<MessageProducerService>();
builder.Services.AddHostedService<MetricsLoggerService>();

var host = builder.Build();

try
{
    await host.RunAsync();
}
finally
{
    Log.CloseAndFlush();
}
