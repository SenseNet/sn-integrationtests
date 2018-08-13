using SenseNet.Diagnostics;

namespace LoggingIntegrationTests.Implementations
{
    internal class TestAuditEvent : IAuditEvent
    {
        public int EventId { get; }
        public string Message { get; }
        public string Title { get; }

        public TestAuditEvent(string message, string title = null, int eventId = 0)
        {
            Message = message;
            Title = title ?? message;
            EventId = eventId;
        }
    }
}
