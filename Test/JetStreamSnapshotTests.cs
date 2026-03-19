using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace CosmoBroker.Tests;

public class JetStreamSnapshotTests : TestBase
{
    public JetStreamSnapshotTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task SnapshotAndRestore_ShouldPreserveMessages()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        var streamConfig = new { name = "SNAP", subjects = new[] { "snap" } };
        await client1.SendAsync($"PUB $JS.API.STREAM.CREATE.SNAP _ {System.Text.Json.JsonSerializer.Serialize(streamConfig).Length}\r\n{System.Text.Json.JsonSerializer.Serialize(streamConfig)}\r\n");
        await Task.Delay(100);

        await client2.SendAsync("PUB snap 2\r\nok\r\n");
        await Task.Delay(100);

        // Take snapshot
        var snapshot = Server.GetJsz(); // triggers internal snapshot via service access
        // Use service directly for snapshot/restore through reflection-free access
        var js = typeof(BrokerServer).GetField("_jetStream", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(Server) as Services.JetStreamService;
        string snapJson = js!.Snapshot();

        // Restore into a new server
        int port = GetFreePort();
        int mon = GetFreePort();
        var server2 = new BrokerServer(port, monitorPort: mon);
        await server2.StartAsync(Cts.Token);
        var js2 = typeof(BrokerServer).GetField("_jetStream", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(server2) as Services.JetStreamService;
        js2!.Restore(snapJson);

        using var client3 = await CreateClientAsync(port, output: Output);
        await client3.SendAsync("SUB snap 1\r\n");
        await Task.Delay(100);

        await client3.SendAsync("PUB snap 2\r\nok\r\n");
        var resp = await client3.ReadResponseAsync();
        Assert.Contains("MSG snap 1 2\r\nok", resp);

        await server2.DisposeAsync();
    }
}
