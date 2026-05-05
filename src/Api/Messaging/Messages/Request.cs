namespace MemoryMessaging.Api.Messaging.Messages;

public sealed record Request(
    Guid MessageId,
    DateTimeOffset SentAt,
    string Payload);
