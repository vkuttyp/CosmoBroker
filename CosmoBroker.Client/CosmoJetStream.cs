using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace CosmoBroker.Client;

public class CosmoJetStream
{
    private readonly CosmoClient _client;
    private readonly string _prefix;

    public CosmoJetStream(CosmoClient client, string prefix = "$JS.API")
    {
        _client = client;
        _prefix = prefix;
    }

    public async Task CreateStreamAsync(StreamConfig config, CancellationToken ct = default)
    {
        var subject = $"{_prefix}.STREAM.CREATE.{config.Name}";
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(config));

        var reply = await _client.RequestAsync(subject, bytes, TimeSpan.FromSeconds(5), ct);
        var response = JsonSerializer.Deserialize<JsApiResponse>(reply.GetStringData());
        if (response?.Error != null)
            throw new Exception($"JetStream error {response.Error.Code}: {response.Error.Description}");
    }

    /// <summary>
    /// Creates the stream if it does not exist. If it already exists the call is a no-op.
    /// </summary>
    public async Task EnsureStreamAsync(StreamConfig config, CancellationToken ct = default)
    {
        var subject = $"{_prefix}.STREAM.CREATE.{config.Name}";
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(config));
        var reply = await _client.RequestAsync(subject, bytes, TimeSpan.FromSeconds(5), ct);
        var response = JsonSerializer.Deserialize<JsApiResponse>(reply.GetStringData());
        // Code 400 means stream already exists — treat as success
        if (response?.Error != null && response.Error.Code != 400)
            throw new Exception($"JetStream error {response.Error.Code}: {response.Error.Description}");
    }

    /// <summary>
    /// Returns the last message published to <paramref name="subject"/> in the stream,
    /// or <c>default</c> if no message exists.
    /// </summary>
    public async Task<T?> GetLastMessageAsync<T>(string stream, string subject, CancellationToken ct = default)
    {
        var reqSubject = $"{_prefix}.STREAM.MSG.GET.{stream}";
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { last_by_subj = subject }));
        var reply = await _client.RequestAsync(reqSubject, body, TimeSpan.FromSeconds(5), ct);
        var response = JsonSerializer.Deserialize<JsGetMsgResponse>(reply.GetStringData());
        if (response?.Error != null || response?.Message?.Data == null) return default;
        var json = Encoding.UTF8.GetString(Convert.FromBase64String(response.Message.Data));
        return JsonSerializer.Deserialize<T>(json);
    }

    public async Task<JsPubAck> PublishAsync(string subject, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var reply = await _client.RequestAsync(subject, payload, TimeSpan.FromSeconds(5), ct);
        var ack = JsonSerializer.Deserialize<JsPubAck>(reply.GetStringData())
                  ?? throw new Exception("JetStream publish: empty ack response.");
        if (ack.Error != null)
            throw new Exception($"JetStream error {ack.Error.Code}: {ack.Error.Description}");
        return ack;
    }
}

public class StreamConfig
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("subjects")]
    public List<string>? Subjects { get; set; }

    /// <summary>Maximum age of messages in nanoseconds. 0 means unlimited.</summary>
    [JsonPropertyName("max_age")]
    public long MaxAge { get; set; }

    /// <summary>"file" (default) or "memory".</summary>
    [JsonPropertyName("storage")]
    public string Storage { get; set; } = "file";

    [JsonPropertyName("deny_subjects")]
    public List<string>? DenySubjects { get; set; }
}

public class JsPubAck
{
    [JsonPropertyName("stream")]
    public string Stream { get; set; } = "";

    [JsonPropertyName("seq")]
    public ulong Seq { get; set; }

    [JsonPropertyName("duplicate")]
    public bool Duplicate { get; set; }

    [JsonPropertyName("error")]
    public JsApiError? Error { get; set; }
}

internal class JsApiResponse
{
    [JsonPropertyName("error")]
    public JsApiError? Error { get; set; }
}

internal class JsGetMsgResponse
{
    [JsonPropertyName("message")]
    public JsMessage? Message { get; set; }

    [JsonPropertyName("error")]
    public JsApiError? Error { get; set; }
}

internal class JsMessage
{
    /// <summary>Base64-encoded message payload.</summary>
    [JsonPropertyName("data")]
    public string? Data { get; set; }
}

public class JsApiError
{
    [JsonPropertyName("code")]
    public int Code { get; set; }

    [JsonPropertyName("description")]
    public string Description { get; set; } = "";
}
