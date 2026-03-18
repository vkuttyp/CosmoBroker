using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace CosmoBroker.Services;

public class MonitoringService
{
    private readonly BrokerServer _server;
    private readonly int _port;
    private Socket? _listenSocket;

    public MonitoringService(BrokerServer server, int port)
    {
        _server = server;
        _port = port;
    }

    public void Start(CancellationToken ct)
    {
        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.Bind(new IPEndPoint(IPAddress.Any, _port));
        _listenSocket.Listen(10);

        Console.WriteLine($"[Monitoring] HTTP Stats listening on port {_port}...");
        _ = AcceptLoopAsync(ct);
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var socket = await _listenSocket!.AcceptAsync(ct);
                _ = HandleRequestAsync(socket);
            }
        }
        catch { }
    }

    private async Task HandleRequestAsync(Socket socket)
    {
        try
        {
            using var stream = new NetworkStream(socket, ownsSocket: true);
            byte[] buffer = new byte[4096];
            int read = await stream.ReadAsync(buffer);
            if (read == 0) return;

            string request = Encoding.UTF8.GetString(buffer, 0, read);
            string line = request.Split("\r\n")[0];
            string[] parts = line.Split(' ');
            if (parts.Length < 2) return;

            string path = parts[1];
            object? responseData = null;

            if (path == "/varz")
            {
                responseData = _server.GetVarz();
            }
            else if (path == "/connz")
            {
                responseData = _server.GetConnz();
            }
            else if (path == "/jsz")
            {
                responseData = _server.GetJsz();
            }
            else
            {
                responseData = new { error = "Not Found", paths = new[] { "/varz", "/connz", "/jsz" } };
            }

            string json = JsonSerializer.Serialize(responseData, new JsonSerializerOptions { WriteIndented = true });
            byte[] jsonBytes = Encoding.UTF8.GetBytes(json);

            string headers = "HTTP/1.1 200 OK\r\n" +
                             "Content-Type: application/json\r\n" +
                             $"Content-Length: {jsonBytes.Length}\r\n" +
                             "Connection: close\r\n\r\n";
            
            await stream.WriteAsync(Encoding.UTF8.GetBytes(headers));
            await stream.WriteAsync(jsonBytes);
            await stream.FlushAsync();
        }
        catch { }
    }
}
