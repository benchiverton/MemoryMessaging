using MemoryMessaging.Api.Messaging.Messages;

namespace MemoryMessaging.Api.Messaging;

public sealed class BounceRequestHandler : IRequestHandler
{
    public ValueTask<Response> Handle(Request request, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        var response = new Response(
            request.MessageId,
            request.SentAt,
            now,
            DateTimeOffset.UtcNow,
            request.Payload);

        return ValueTask.FromResult(response);
    }
}



