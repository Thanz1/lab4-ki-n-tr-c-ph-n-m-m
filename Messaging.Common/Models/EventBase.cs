using System;

namespace Messaging.Common.Models
{
    public abstract class EventBase
    {
        public Guid EventId { get; private set; }
        public DateTime Timestamp { get; private set; }
        public string CorrelationId { get; set; } = string.Empty;

        protected EventBase()
        {
            EventId = Guid.NewGuid();
            Timestamp = DateTime.UtcNow;
        }
    }
}
