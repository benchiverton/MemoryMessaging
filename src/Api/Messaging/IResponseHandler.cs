using MemoryMessaging.Api.Messaging.Messages;

namespace MemoryMessaging.Api.Messaging;

public interface IResponseHandler
{
    public ValueTask Handle(Response response, CancellationToken cancellationToken);
}
