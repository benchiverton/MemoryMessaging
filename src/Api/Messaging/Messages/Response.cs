namespace MemoryMessaging.Api.Messaging.Messages;

public sealed record Response(
    Guid MessageId,
    DateTimeOffset RequestSentAt,
    DateTimeOffset ConsumerReceivedAt,
    DateTimeOffset ConsumerSentAt,
    string Payload);
