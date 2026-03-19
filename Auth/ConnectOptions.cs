using System.Text.Json.Serialization;

namespace CosmoBroker.Auth;

public class ConnectOptions
{
    [JsonPropertyName("user")]
    public string? User { get; set; }

    [JsonPropertyName("pass")]
    public string? Pass { get; set; }

    [JsonPropertyName("auth_token")]
    public string? AuthToken { get; set; }

    [JsonPropertyName("jwt")]
    public string? Jwt { get; set; }

    [JsonPropertyName("nkey")]
    public string? Nkey { get; set; }

    [JsonPropertyName("sig")]
    public string? Sig { get; set; }

    [JsonPropertyName("verbose")]
    public bool Verbose { get; set; }

    [JsonPropertyName("pedantic")]
    public bool Pedantic { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("lang")]
    public string? Lang { get; set; }

    [JsonPropertyName("version")]
    public string? Version { get; set; }

    [JsonPropertyName("no_echo")]
    public bool NoEcho { get; set; }
}
