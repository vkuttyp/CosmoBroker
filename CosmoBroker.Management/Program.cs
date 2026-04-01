using CosmoApiServer.Core.Hosting;
using CosmoBroker.Management.Models;
using CosmoBroker.Management.Security;
using CosmoBroker.Management.Services;
using Microsoft.Extensions.DependencyInjection;

var builder = CosmoWebApplicationBuilder.Create(args);

int managementPort = 9091;
var envManagementPort = Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_PORT");
if (!string.IsNullOrWhiteSpace(envManagementPort) && int.TryParse(envManagementPort, out var parsedManagementPort))
    managementPort = parsedManagementPort;

string monitorBaseUrl = Environment.GetEnvironmentVariable("COSMOBROKER_MONITOR_URL")
    ?? "http://127.0.0.1:8222";
string managementUsername = Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_USERNAME") ?? string.Empty;
string managementPassword = Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_PASSWORD") ?? string.Empty;
string managementCertificatePath = Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_CERT_PATH") ?? string.Empty;
string managementCertificatePassword = Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_CERT_PASSWORD") ?? string.Empty;
bool enableHttp3 = string.Equals(
    Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_ENABLE_HTTP3"),
    "true",
    StringComparison.OrdinalIgnoreCase);
bool allowAnonymousHealth = !string.Equals(
    Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_ALLOW_ANONYMOUS_HEALTH"),
    "false",
    StringComparison.OrdinalIgnoreCase);

var staticRoot = Path.Combine(AppContext.BaseDirectory, "wwwroot");
if (!Directory.Exists(staticRoot))
    staticRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../wwwroot"));

builder.ListenOn(managementPort);
if (!string.IsNullOrWhiteSpace(managementCertificatePath))
{
    builder.UseHttps(managementCertificatePath, managementCertificatePassword);
    if (enableHttp3)
        builder.UseHttp3();
}
builder.UseExceptionHandler();

builder.Services.AddSingleton(new BrokerManagementOptions
{
    MonitorBaseUrl = monitorBaseUrl
});
builder.Services.AddSingleton(new ManagementAuthOptions
{
    Enabled = !string.IsNullOrWhiteSpace(managementUsername) && !string.IsNullOrWhiteSpace(managementPassword),
    Username = managementUsername,
    Password = managementPassword,
    AllowAnonymousHealth = allowAnonymousHealth
});
builder.Services.AddSingleton<BrokerMonitorClient>();
builder.UseMiddleware<ManagementBasicAuthMiddleware>();
builder.UseStaticFiles(staticRoot);
builder.AddRazorComponents();

var app = builder.Build();

app.MapGet("/api/health", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var health = await client.GetHealthAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(health);
});

app.MapGet("/api/overview", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var snapshot = await client.GetSnapshotAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(snapshot);
});

app.MapGet("/api/varz", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var snapshot = await client.GetSnapshotAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(snapshot.varz);
});

app.MapGet("/api/connections", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var connections = await client.GetConnectionsAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(connections);
});

app.MapGet("/api/routes", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var snapshot = await client.GetSnapshotAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(snapshot.routez);
});

app.MapGet("/api/gateways", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var snapshot = await client.GetSnapshotAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(snapshot.gatewayz);
});

app.MapGet("/api/leafs", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var snapshot = await client.GetSnapshotAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(snapshot.leafz);
});

app.MapGet("/api/jetstream", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var jetStream = await client.GetJetStreamAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(jetStream);
});

app.MapGet("/api/rabbitmq", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var rabbitMq = await client.GetRabbitMqAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(rabbitMq);
});

app.MapGet("/api/rabbitmq/super-streams", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var summaries = await client.GetSuperStreamsAsync(ctx.RequestAborted);
    ctx.Response.WriteJson(summaries);
});

app.MapGet("/api/rabbitmq/super-stream", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    string vhost = ctx.Request.Query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue)
        ? vhostValue
        : "/";
    string name = ctx.Request.Query.TryGetValue("name", out var nameValue) ? nameValue : string.Empty;
    var summary = await client.GetSuperStreamAsync(vhost, name, ctx.RequestAborted);
    if (summary == null)
    {
        ctx.Response.StatusCode = 404;
        ctx.Response.ReasonPhrase = "Not Found";
        ctx.Response.WriteJson(new { error = $"Super stream '{name}' was not found in vhost '{vhost}'." });
        return;
    }

    ctx.Response.WriteJson(summary);
});

app.MapGet("/api/rabbitmq/super-streams/route", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var request = new SuperStreamRoutePreviewRequest
    {
        vhost = ctx.Request.Query.TryGetValue("vhost", out var vhostValue) && !string.IsNullOrWhiteSpace(vhostValue) ? vhostValue : "/",
        exchange = ctx.Request.Query.TryGetValue("exchange", out var exchangeValue) ? exchangeValue : string.Empty,
        routing_key = ctx.Request.Query.TryGetValue("routing_key", out var routingKeyValue) ? routingKeyValue : string.Empty,
        partition_key = ctx.Request.Query.TryGetValue("partition_key", out var partitionKeyValue) ? partitionKeyValue : null
    };

    var result = await client.PreviewSuperStreamRouteAsync(request, ctx.RequestAborted);
    if (!result.ok)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ReasonPhrase = "Bad Request";
    }
    ctx.Response.WriteJson(result);
});

app.MapPost("/api/rabbitmq/streams/reset-offset", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var request = ctx.Request.ReadJson<StreamOffsetResetRequest>() ?? new StreamOffsetResetRequest();
    var result = await client.ResetStreamOffsetAsync(request, ctx.RequestAborted);
    if (!result.ok)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ReasonPhrase = "Bad Request";
    }
    ctx.Response.WriteJson(result);
});

app.MapPost("/api/rabbitmq/super-streams/reset-offset", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var request = ctx.Request.ReadJson<SuperStreamOffsetResetRequest>() ?? new SuperStreamOffsetResetRequest();
    var result = await client.ResetSuperStreamOffsetAsync(request, ctx.RequestAborted);
    if (!result.ok)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ReasonPhrase = "Bad Request";
    }
    ctx.Response.WriteJson(result);
});

app.MapPost("/rabbitmq/streams/reset-offset", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var form = ctx.Request.ReadForm();
    var request = new StreamOffsetResetRequest
    {
        vhost = form.Fields.TryGetValue("vhost", out var vhost) && !string.IsNullOrWhiteSpace(vhost) ? vhost : "/",
        queue = form.Fields.TryGetValue("queue", out var queue) ? queue : string.Empty,
        consumer = form.Fields.TryGetValue("consumer", out var consumer) ? consumer : string.Empty,
        offset = form.Fields.TryGetValue("offset", out var offset) && !string.IsNullOrWhiteSpace(offset) ? offset : "next"
    };

    var result = await client.ResetStreamOffsetAsync(request, ctx.RequestAborted);
    var message = result.ok
        ? $"Reset {request.consumer} on {request.queue} to next offset {result.next_offset}."
        : (result.error ?? "Unable to reset stream offset.");
    var query = $"status={(result.ok ? "ok" : "error")}&message={Uri.EscapeDataString(message)}";
    ctx.Response.StatusCode = 302;
    ctx.Response.ReasonPhrase = "Found";
    ctx.Response.Headers["Location"] = $"/rabbitmq?{query}";
});

app.MapPost("/rabbitmq/super-streams/reset-offset", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var form = ctx.Request.ReadForm();
    var request = new SuperStreamOffsetResetRequest
    {
        vhost = form.Fields.TryGetValue("super-vhost", out var vhost) && !string.IsNullOrWhiteSpace(vhost) ? vhost : "/",
        exchange = form.Fields.TryGetValue("super-exchange", out var exchange) ? exchange : string.Empty,
        consumer = form.Fields.TryGetValue("super-consumer", out var consumer) ? consumer : string.Empty,
        offset = form.Fields.TryGetValue("super-offset", out var offset) && !string.IsNullOrWhiteSpace(offset) ? offset : "next"
    };

    var result = await client.ResetSuperStreamOffsetAsync(request, ctx.RequestAborted);
    var message = result.ok
        ? $"Reset {request.consumer} on {request.exchange} across {result.partitions.Count} partitions."
        : (result.error ?? "Unable to reset super stream offsets.");
    var query = $"status={(result.ok ? "ok" : "error")}&message={Uri.EscapeDataString(message)}";
    ctx.Response.StatusCode = 302;
    ctx.Response.ReasonPhrase = "Found";
    ctx.Response.Headers["Location"] = $"/rabbitmq?{query}";
});

app.MapPost("/rabbitmq/super-streams/route-preview", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var form = ctx.Request.ReadForm();
    var request = new SuperStreamRoutePreviewRequest
    {
        vhost = form.Fields.TryGetValue("route-vhost", out var vhost) && !string.IsNullOrWhiteSpace(vhost) ? vhost : "/",
        exchange = form.Fields.TryGetValue("route-exchange", out var exchange) ? exchange : string.Empty,
        routing_key = form.Fields.TryGetValue("route-routing-key", out var routingKey) ? routingKey : string.Empty,
        partition_key = form.Fields.TryGetValue("route-partition-key", out var partitionKey) && !string.IsNullOrWhiteSpace(partitionKey) ? partitionKey : null
    };

    var result = await client.PreviewSuperStreamRouteAsync(request, ctx.RequestAborted);
    var message = result.ok
        ? $"Super stream route resolves to {result.partition}."
        : (result.error ?? "Unable to preview super stream route.");
    var query = $"status={(result.ok ? "ok" : "error")}&message={Uri.EscapeDataString(message)}";
    if (!string.IsNullOrWhiteSpace(request.exchange))
        query += $"&super={Uri.EscapeDataString(request.exchange)}&vhost={Uri.EscapeDataString(request.vhost)}";
    ctx.Response.StatusCode = 302;
    ctx.Response.ReasonPhrase = "Found";
    ctx.Response.Headers["Location"] = $"/rabbitmq?{query}";
});

app.MapPost("/api/rabbitmq/streams/retention", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var request = ctx.Request.ReadJson<StreamRetentionRequest>() ?? new StreamRetentionRequest();
    var result = await client.SetStreamRetentionAsync(request, ctx.RequestAborted);
    if (!result.ok)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ReasonPhrase = "Bad Request";
    }
    ctx.Response.WriteJson(result);
});

app.MapPost("/api/rabbitmq/super-streams/retention", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var request = ctx.Request.ReadJson<SuperStreamRetentionRequest>() ?? new SuperStreamRetentionRequest();
    var result = await client.SetSuperStreamRetentionAsync(request, ctx.RequestAborted);
    if (!result.ok)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ReasonPhrase = "Bad Request";
    }
    ctx.Response.WriteJson(result);
});

app.MapPost("/rabbitmq/streams/retention", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var form = ctx.Request.ReadForm();
    var request = new StreamRetentionRequest
    {
        vhost = form.Fields.TryGetValue("retention-vhost", out var vhost) && !string.IsNullOrWhiteSpace(vhost) ? vhost : "/",
        queue = form.Fields.TryGetValue("retention-queue", out var queue) ? queue : string.Empty,
        max_length_messages = ParseNullableLong(form.Fields.TryGetValue("retention-max-messages", out var maxLength) ? maxLength : null),
        max_length_bytes = ParseNullableLong(form.Fields.TryGetValue("retention-max-bytes", out var maxBytes) ? maxBytes : null),
        max_age_ms = ParseNullableLong(form.Fields.TryGetValue("retention-max-age-ms", out var maxAge) ? maxAge : null)
    };

    var result = await client.SetStreamRetentionAsync(request, ctx.RequestAborted);
    var message = result.ok
        ? $"Updated stream retention on {request.queue}."
        : (result.error ?? "Unable to update stream retention.");
    var query = $"status={(result.ok ? "ok" : "error")}&message={Uri.EscapeDataString(message)}";
    ctx.Response.StatusCode = 302;
    ctx.Response.ReasonPhrase = "Found";
    ctx.Response.Headers["Location"] = $"/rabbitmq?{query}";
});

app.MapPost("/rabbitmq/super-streams/retention", async ctx =>
{
    var client = ctx.RequestServices.GetRequiredService<BrokerMonitorClient>();
    var form = ctx.Request.ReadForm();
    var request = new SuperStreamRetentionRequest
    {
        vhost = form.Fields.TryGetValue("super-retention-vhost", out var vhost) && !string.IsNullOrWhiteSpace(vhost) ? vhost : "/",
        exchange = form.Fields.TryGetValue("super-retention-exchange", out var exchange) ? exchange : string.Empty,
        max_length_messages = ParseNullableLong(form.Fields.TryGetValue("super-retention-max-messages", out var maxLength) ? maxLength : null),
        max_length_bytes = ParseNullableLong(form.Fields.TryGetValue("super-retention-max-bytes", out var maxBytes) ? maxBytes : null),
        max_age_ms = ParseNullableLong(form.Fields.TryGetValue("super-retention-max-age-ms", out var maxAge) ? maxAge : null)
    };

    var result = await client.SetSuperStreamRetentionAsync(request, ctx.RequestAborted);
    var message = result.ok
        ? $"Updated retention on {request.exchange}."
        : (result.error ?? "Unable to update super stream retention.");
    var query = $"status={(result.ok ? "ok" : "error")}&message={Uri.EscapeDataString(message)}";
    if (!string.IsNullOrWhiteSpace(request.exchange))
        query += $"&super={Uri.EscapeDataString(request.exchange)}&vhost={Uri.EscapeDataString(request.vhost)}";
    ctx.Response.StatusCode = 302;
    ctx.Response.ReasonPhrase = "Found";
    ctx.Response.Headers["Location"] = $"/rabbitmq?{query}";
});

Console.WriteLine($"CosmoBroker.Management running on http://localhost:{managementPort}");
Console.WriteLine($"Tracking broker monitor endpoint: {monitorBaseUrl}");
app.Run();

static long? ParseNullableLong(string? value)
{
    if (string.IsNullOrWhiteSpace(value))
        return null;
    return long.TryParse(value, out var result) ? result : null;
}
