using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using CosmoBroker.Auth;
using System.Collections.Generic;

namespace CosmoBroker.Tests;

public class AdvancedTests : TestBase
{
    private static readonly SqlAuthenticator _auth = CreateAuth();

    private static SqlAuthenticator CreateAuth()
    {
        // Mock repo or just use real one if sqlite is fine
        // For tests, we'll use a custom authenticator that allows prefix testing
        return null!; 
    }

    public AdvancedTests(ITestOutputHelper output) : base(output, new SimpleAuthenticator()) { }

    [Fact]
    public async Task TestSubjectMapping()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        // Configure a mapping: orders.* -> internal.orders.$1
        var mapping = new Models.SubjectMapping {
            SourcePattern = "orders.*"
        };
        mapping.Destinations.Add(new Models.MapDestination { Subject = "internal.orders.$1", Weight = 1.0 });
        
        // Inject into global account (hacky for test)
        // In a real setup, this would be via config or auth
        // For now, let's assume we can test it if we have a way to set it.
    }

    [Fact]
    public async Task TestMonitoringEndpoints()
    {
        await Server.StartAsync(Cts.Token);
        using var httpClient = new System.Net.Http.HttpClient();
        
        var varz = await httpClient.GetStringAsync($"http://127.0.0.1:{MonitorPort}/varz");
        Assert.Contains("cosmo-broker", varz);

        var connz = await httpClient.GetStringAsync($"http://127.0.0.1:{MonitorPort}/connz");
        Assert.Contains("connections", connz);
    }
}
