using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace CosmoBroker.Tests;

public class QueueGroupTests : TestBase
{
    public QueueGroupTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task QueueGroup_ShouldDeliverToOneMember_EvenIfPublisherIsMember()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        await client1.SendAsync("SUB foo workers 1\r\n");
        await client2.SendAsync("SUB foo workers 2\r\n");
        await Task.Delay(100);

        // Publish from a queue member.
        await client1.SendAsync("PUB foo 5\r\nhello\r\n");

        var r1 = await client1.ReadResponseAsync(500);
        var r2 = await client2.ReadResponseAsync(500);

        Assert.True(r1.Contains("MSG foo") || r2.Contains("MSG foo"));
    }

    [Fact]
    public async Task NoEcho_ShouldNotReceiveOwnPublish()
    {
        await Server.StartAsync(Cts.Token);
        using var client1 = await CreateClientAsync();
        using var client2 = await CreateClientAsync();

        await client1.SendAsync("CONNECT {\"no_echo\":true}\r\n");
        await client2.SendAsync("CONNECT {}\r\n");
        await Task.Delay(50);

        await client1.SendAsync("SUB foo 1\r\n");
        await client2.SendAsync("SUB foo 2\r\n");
        await Task.Delay(50);

        await client1.SendAsync("PUB foo 5\r\nhello\r\n");

        var r1 = await client1.ReadResponseAsync(300);
        var r2 = await client2.ReadResponseAsync(300);

        Assert.DoesNotContain("MSG foo 1", r1);
        Assert.Contains("MSG foo 2", r2);
    }
}
