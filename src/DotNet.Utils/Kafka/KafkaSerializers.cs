using Confluent.Kafka;

namespace CacheManager.Kafka
{
    public class KafkaSerializers
    {
        public static MessagePackSerializer<T> CreateMessagePackSerializer<T>() => new();
        public static MessagePackDeserializer<T> CreateMessagePackDeserializer<T>() => new();

        public class MessagePackSerializer<T> : ISerializer<T>
        {
            public byte[] Serialize(T data, SerializationContext context)
            {
                if (data is null)
                {
                    return default;
                }
                var bytes = MessagePack.MessagePackSerializer.Serialize<T>(data);
                return bytes;
            }
        }
        public class MessagePackDeserializer<T> : IDeserializer<T>
        {
            public T Deserialize(ReadOnlySpan<byte> bytes, bool isNull, SerializationContext context)
            {
                if (isNull || bytes.IsEmpty)
                {
                    return default;
                }
                var data = MessagePack.MessagePackSerializer.Deserialize<T>(bytes.ToArray());
                return data;
            }
        }
    }
}
