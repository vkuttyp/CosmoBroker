using System.Net.Http.Json;
using CosmoBroker.Management.Models;

namespace CosmoBroker.Management.Services;

public sealed class BrokerMonitorClient
{
    private readonly HttpClient _httpClient;
    private readonly BrokerManagementOptions _options;

    public BrokerMonitorClient(BrokerManagementOptions options)
    {
        _options = options;
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(AppendTrailingSlash(options.MonitorBaseUrl)),
            Timeout = TimeSpan.FromSeconds(5)
        };
    }

    public async Task<BrokerHealth> GetHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _httpClient.GetAsync("varz", cancellationToken);
            response.EnsureSuccessStatusCode();
            return new BrokerHealth
            {
                ok = true,
                monitor_url = _options.MonitorBaseUrl,
                fetched_at_utc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            return new BrokerHealth
            {
                ok = false,
                monitor_url = _options.MonitorBaseUrl,
                error = ex.Message,
                fetched_at_utc = DateTime.UtcNow
            };
        }
    }

    public async Task<BrokerSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var health = await GetHealthAsync(cancellationToken);
        if (!health.ok)
            return new BrokerSnapshot { health = health };

        var varzTask = GetAsync<VarzStats>("varz", cancellationToken);
        var connzTask = GetAsync<ConnectionList>("connz", cancellationToken);
        var routezTask = GetAsync<RouteStats>("routez", cancellationToken);
        var gatewayzTask = GetAsync<GatewayStats>("gatewayz", cancellationToken);
        var leafzTask = GetAsync<LeafStats>("leafz", cancellationToken);
        var jszTask = GetAsync<JetStreamStats>("jsz", cancellationToken);
        var rmqzTask = GetAsync<RabbitMqStats>("rmqz", cancellationToken);

        await Task.WhenAll(varzTask, connzTask, routezTask, gatewayzTask, leafzTask, jszTask, rmqzTask);

        return new BrokerSnapshot
        {
            health = health,
            varz = await varzTask,
            connz = await connzTask ?? new ConnectionList(),
            routez = await routezTask ?? new RouteStats(),
            gatewayz = await gatewayzTask ?? new GatewayStats(),
            leafz = await leafzTask ?? new LeafStats(),
            jsz = await jszTask ?? new JetStreamStats(),
            rmqz = await rmqzTask ?? new RabbitMqStats()
        };
    }

    public async Task<ConnectionList> GetConnectionsAsync(CancellationToken cancellationToken = default)
        => await GetAsync<ConnectionList>("connz", cancellationToken) ?? new ConnectionList();

    public async Task<JetStreamStats> GetJetStreamAsync(CancellationToken cancellationToken = default)
        => await GetAsync<JetStreamStats>("jsz", cancellationToken) ?? new JetStreamStats();

    public async Task<RabbitMqStats> GetRabbitMqAsync(CancellationToken cancellationToken = default)
        => await GetAsync<RabbitMqStats>("rmqz", cancellationToken) ?? new RabbitMqStats();

    private async Task<T?> GetAsync<T>(string relativePath, CancellationToken cancellationToken)
    {
        using var response = await _httpClient.GetAsync(relativePath, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken);
    }

    private static string AppendTrailingSlash(string baseUrl)
        => baseUrl.EndsWith("/", StringComparison.Ordinal) ? baseUrl : baseUrl + "/";
}
