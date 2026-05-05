using MemoryMessaging.Api.Messaging.Messages;

namespace MemoryMessaging.Api.Messaging;

public interface IRequestHandler
{
    public ValueTask<Response> Handle(Request request, CancellationToken cancellationToken);
}
