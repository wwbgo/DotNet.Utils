namespace DotNet.Utils
{
    public class AppSettings
    {
        public int ClusterId { get; set; }

        public class KafkaSettings
        {
            public string Servers { get; set; }
            public string GroupId { get; set; }
            public string TopicPrefix { get; set; } = "";
            public int MaxPartitions { get; set; } = 64;
        }
    }
}
