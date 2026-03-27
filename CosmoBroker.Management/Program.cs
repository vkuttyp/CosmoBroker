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
bool allowAnonymousHealth = !string.Equals(
    Environment.GetEnvironmentVariable("COSMOBROKER_MANAGEMENT_ALLOW_ANONYMOUS_HEALTH"),
    "false",
    StringComparison.OrdinalIgnoreCase);

var staticRoot = Path.Combine(AppContext.BaseDirectory, "wwwroot");
if (!Directory.Exists(staticRoot))
    staticRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../wwwroot"));

builder.ListenOn(managementPort);
builder.UseExceptionHandler();
builder.UseStaticFiles(staticRoot);
builder.AddRazorComponents();

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

Console.WriteLine($"CosmoBroker.Management running on http://localhost:{managementPort}");
Console.WriteLine($"Tracking broker monitor endpoint: {monitorBaseUrl}");
app.Run();
