namespace MemoryMessaging.Api.Messaging.Options;

public sealed class MessagingOptions
{
    public const string SectionName = "Messaging";

    public List<ConsumerOptions> Consumers { get; set; } = [];

    public List<ProducerOptions> Producers { get; set; } = [];
}
