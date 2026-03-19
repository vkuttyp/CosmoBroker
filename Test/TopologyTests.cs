using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace CosmoBroker.Tests;

public class TopologyTests : TestBase
{
    public TopologyTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task Leafnode_ShouldForwardHubMessagesToLocalSubs()
    {
        int hubPort = GetFreePort();
        int hubMon = GetFreePort();
        var hub = new BrokerServer(hubPort, monitorPort: hubMon);
        await hub.StartAsync();

        int leafPort = GetFreePort();
        int leafMon = GetFreePort();
        var leaf = new BrokerServer(leafPort, monitorPort: leafMon);
        leaf.AddLeafnodeHub($"nats://127.0.0.1:{hubPort}");
        await leaf.StartAsync();

        using var leafClient = await CreateClientAsync(leafPort, output: Output);
        await leafClient.SendAsync("SUB foo 1\r\n");

        // Wait for leaf to propagate subscription to hub.
        for (int i = 0; i < 20; i++)
        {
            if (hub.HasSubscribers("foo")) break;
            await Task.Delay(50);
        }

        using var hubClient = await CreateClientAsync(hubPort, output: Output);
        await hubClient.SendAsync("PUB foo 5\r\nhello\r\n");

        var resp = await leafClient.ReadResponseAsync();
        Assert.Contains("MSG foo 1 5\r\nhello", resp);

        await leaf.DisposeAsync();
        await hub.DisposeAsync();
    }

    [Fact]
    public async Task Route_ShouldForwardAcrossBrokers()
    {
        int aPort = GetFreePort();
        int aMon = GetFreePort();
        var a = new BrokerServer(aPort, monitorPort: aMon);

        int bPort = GetFreePort();
        int bMon = GetFreePort();
        var b = new BrokerServer(bPort, monitorPort: bMon);

        // Bidirectional peers so both sides have outbound routes.
        a.AddPeer("127.0.0.1", bPort);
        b.AddPeer("127.0.0.1", aPort);

        await a.StartAsync();
        await b.StartAsync();

        using var clientB = await CreateClientAsync(bPort, output: Output);
        await clientB.SendAsync("SUB foo 1\r\n");

        // Wait for route propagation.
        for (int i = 0; i < 20; i++)
        {
            if (a.HasSubscribers("foo")) break;
            await Task.Delay(50);
        }

        using var clientA = await CreateClientAsync(aPort, output: Output);
        await clientA.SendAsync("PUB foo 5\r\nhello\r\n");

        var resp = await clientB.ReadResponseAsync();
        Assert.Contains("MSG foo 1 5\r\nhello", resp);

        await a.DisposeAsync();
        await b.DisposeAsync();
    }
}
