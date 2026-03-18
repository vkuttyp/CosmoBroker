using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using CosmoBroker.Auth;

namespace CosmoBroker.Tests;

public class AuthTests : TestBase
{
    private static readonly SimpleAuthenticator _auth = new SimpleAuthenticator("derek", "foobar");

    public AuthTests(ITestOutputHelper output) : base(output, _auth) { }

    [Fact]
    public async Task TestNoAuthClient()
    {
        await Server.StartAsync(Cts.Token);
        using var client = await CreateClientAsync();
        
        // Initial INFO should say auth_required: true
        // We already read it in CreateClientAsync, but let's send a PUB and expect error
        await client.SendAsync("PUB foo 2\r\nok\r\n");
        var resp = await client.ReadResponseAsync();
        Assert.Contains("-ERR", resp);
    }

    [Fact]
    public async Task TestBadUserClient()
    {
        await Server.StartAsync(Cts.Token);
        using var client = await CreateClientAsync();
        
        var connect = new { user = "wrong", pass = "foobar" };
        await client.SendAsync($"CONNECT {System.Text.Json.JsonSerializer.Serialize(connect)}\r\n");
        
        var resp = await client.ReadResponseAsync();
        Assert.Contains("-ERR", resp);
    }

    [Fact]
    public async Task TestGoodConnect()
    {
        await Server.StartAsync(Cts.Token);
        using var client = await CreateClientAsync();
        
        var connect = new { user = "derek", pass = "foobar" };
        await client.SendAsync($"CONNECT {System.Text.Json.JsonSerializer.Serialize(connect)}\r\n");
        
        var resp = await client.ReadResponseAsync();
        Assert.Contains("+OK", resp);
    }
}
