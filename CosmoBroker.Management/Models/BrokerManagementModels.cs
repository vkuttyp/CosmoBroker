using System.Text.Json.Serialization;

namespace CosmoBroker.Management.Models;

public sealed class BrokerManagementOptions
{
    public string MonitorBaseUrl { get; init; } = "http://127.0.0.1:8222";
}

public sealed class ManagementAuthOptions
{
    public bool Enabled { get; init; }
    public string Username { get; init; } = string.Empty;
    public string Password { get; init; } = string.Empty;
    public bool AllowAnonymousHealth { get; init; } = true;
}

public sealed class BrokerHealth
{
    public bool ok { get; set; }
    public string monitor_url { get; set; } = string.Empty;
    public string? error { get; set; }
    public DateTime fetched_at_utc { get; set; }
}

public sealed class BrokerSnapshot
{
    public BrokerHealth health { get; set; } = new();
    public VarzStats? varz { get; set; }
    public ConnectionList connz { get; set; } = new();
    public RouteStats routez { get; set; } = new();
    public GatewayStats gatewayz { get; set; } = new();
    public LeafStats leafz { get; set; } = new();
    public JetStreamStats jsz { get; set; } = new();
    public RabbitMqStats rmqz { get; set; } = new();
}

public sealed class VarzStats
{
    public string? server_id { get; set; }
    public string? version { get; set; }
    public string? uptime { get; set; }
    public int connections { get; set; }
    public long total_connections { get; set; }
    public int routes { get; set; }
    public int port { get; set; }
    public bool tls_enabled { get; set; }
    public bool lame_duck_mode { get; set; }
}

public sealed class ConnectionList
{
    public List<ConnectionStats> connections { get; set; } = [];
}

public sealed class ConnectionStats
{
    public string? protocol { get; set; }
    public string? remote_addr { get; set; }
    public long bytes_in { get; set; }
    public long bytes_out { get; set; }
    public long msg_in { get; set; }
    public long msg_out { get; set; }
    public long msg_drop { get; set; }
    public int subscriptions { get; set; }
    public string? account { get; set; }
}

public sealed class RouteStats
{
    public int routes { get; set; }
}

public sealed class GatewayStats
{
    public int gateways { get; set; }
}

public sealed class LeafStats
{
    public int leafnodes { get; set; }
}

public sealed class JetStreamStats
{
    public List<JetStreamStreamStats> streams { get; set; } = [];
}

public sealed class JetStreamStreamStats
{
    public string? name { get; set; }
    public List<string> subjects { get; set; } = [];
    public int messages { get; set; }
    public long bytes { get; set; }
    public long last_seq { get; set; }
    public int consumers { get; set; }
}

public sealed class RabbitMqStats
{
    public List<RabbitExchangeStats> exchanges { get; set; } = [];
    public List<RabbitQueueStats> queues { get; set; } = [];
}

public sealed class SuperStreamSummary
{
    public string vhost { get; set; } = "/";
    public string name { get; set; } = string.Empty;
    public int partition_count { get; set; }
    public List<string> partitions { get; set; } = [];
    public List<SuperStreamPartitionSummary> partition_details { get; set; } = [];
    public long messages { get; set; }
    public long bytes { get; set; }
    public int consumers { get; set; }
    public int logical_consumers { get; set; }
    public long max_lag { get; set; }
    public long? min_head_offset { get; set; }
    public long? max_tail_offset { get; set; }
    public List<string> retention { get; set; } = [];
    public List<SuperStreamConsumerSummary> consumer_details { get; set; } = [];
}

public sealed class SuperStreamPartitionSummary
{
    public string name { get; set; } = string.Empty;
    public int messages { get; set; }
    public long bytes { get; set; }
    public int consumers { get; set; }
    public long head_offset { get; set; }
    public long tail_offset { get; set; }
    public long max_lag { get; set; }
    public long? max_length_messages { get; set; }
    public long? max_length_bytes { get; set; }
    public long? max_age_ms { get; set; }
}

public sealed class SuperStreamConsumerSummary
{
    public string consumer { get; set; } = string.Empty;
    public int partition_count { get; set; }
    public long total_lag { get; set; }
    public long max_lag { get; set; }
    public long? min_next_offset { get; set; }
    public long? max_next_offset { get; set; }
    public List<SuperStreamConsumerPartitionSummary> partition_details { get; set; } = [];
}

public sealed class SuperStreamConsumerPartitionSummary
{
    public string partition { get; set; } = string.Empty;
    public long next_offset { get; set; }
    public long lag { get; set; }
}

public sealed class StreamOffsetResetRequest
{
    public string vhost { get; set; } = "/";
    public string queue { get; set; } = string.Empty;
    public string consumer { get; set; } = string.Empty;
    public string offset { get; set; } = "next";
}

public sealed class SuperStreamOffsetResetRequest
{
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public string consumer { get; set; } = string.Empty;
    public string offset { get; set; } = "next";
}

public sealed class StreamOffsetResetResult
{
    public bool ok { get; set; }
    public string? error { get; set; }
    public string vhost { get; set; } = "/";
    public string queue { get; set; } = string.Empty;
    public string consumer { get; set; } = string.Empty;
    public long next_offset { get; set; }
}

public sealed class SuperStreamOffsetResetResult
{
    public bool ok { get; set; }
    public string? error { get; set; }
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public string consumer { get; set; } = string.Empty;
    public Dictionary<string, long> partitions { get; set; } = [];
}

public sealed class SuperStreamRoutePreviewRequest
{
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public string routing_key { get; set; } = string.Empty;
    public string? partition_key { get; set; }
}

public sealed class SuperStreamRoutePreviewResult
{
    public bool ok { get; set; }
    public string? error { get; set; }
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public string routing_key { get; set; } = string.Empty;
    public string? partition_key { get; set; }
    public string? partition { get; set; }
}

public sealed class StreamRetentionRequest
{
    public string vhost { get; set; } = "/";
    public string queue { get; set; } = string.Empty;
    public long? max_length_messages { get; set; }
    public long? max_length_bytes { get; set; }
    public long? max_age_ms { get; set; }
}

public sealed class StreamRetentionResult
{
    public bool ok { get; set; }
    public string? error { get; set; }
    public string vhost { get; set; } = "/";
    public string queue { get; set; } = string.Empty;
    public long? max_length_messages { get; set; }
    public long? max_length_bytes { get; set; }
    public long? max_age_ms { get; set; }
}

public sealed class SuperStreamRetentionRequest
{
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public long? max_length_messages { get; set; }
    public long? max_length_bytes { get; set; }
    public long? max_age_ms { get; set; }
}

public sealed class SuperStreamRetentionResult
{
    public bool ok { get; set; }
    public string? error { get; set; }
    public string vhost { get; set; } = "/";
    public string exchange { get; set; } = string.Empty;
    public long? max_length_messages { get; set; }
    public long? max_length_bytes { get; set; }
    public long? max_age_ms { get; set; }
}

public sealed class RabbitExchangeStats
{
    public string? vhost { get; set; }
    public string? name { get; set; }
    public string? type { get; set; }
    public bool durable { get; set; }
    public int? super_stream_partition_count { get; set; }
    public List<string> super_stream_partitions { get; set; } = [];
}

public sealed class RabbitQueueStats
{
    public string? vhost { get; set; }
    public string? name { get; set; }
    public int messages { get; set; }
    public long bytes { get; set; }
    public int consumers { get; set; }
    public int unacked { get; set; }
    public string? queue_type { get; set; }
    public long stream_head_offset { get; set; }
    public long stream_tail_offset { get; set; }
    public long? stream_max_length_messages { get; set; }
    public long? stream_max_length_bytes { get; set; }
    public long? stream_max_age_ms { get; set; }
    public Dictionary<string, long> stream_offsets { get; set; } = [];
    public Dictionary<string, long> stream_consumer_lag { get; set; } = [];
    public bool durable { get; set; }
    public string? dlx { get; set; }
    public int? max_priority { get; set; }
}
