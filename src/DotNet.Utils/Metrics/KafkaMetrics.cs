using Prometheus;

namespace DotNet.Utils
{
    public class KafkaMetrics : IKafkaMetrics
    {
        public static readonly Counter _counters = Metrics.CreateCounter("kafka_consumer_count", "kafka consumer data count.", "group_id", "topic", "type");

        private Counter.Child _handleCounter;
        private Counter.Child _commitCounter;

        public IKafkaMetrics SetLabels(string groupId, string topic)
        {
            _handleCounter = _counters.WithLabels(new[] { groupId, topic, "handle" });
            _commitCounter = _counters.WithLabels(new[] { groupId, topic, "commit" });
            return this;
        }

        public void HandlerInc(int increment = 1)
        {
            _handleCounter.Inc(increment);
        }
        public void CommitInc(int increment = 1)
        {
            _commitCounter.Inc(increment);
        }
    }

    public interface IKafkaMetrics
    {
        IKafkaMetrics SetLabels(string groupId, string topic);

        void HandlerInc(int increment = 1);
        void CommitInc(int increment = 1);
    }
}
