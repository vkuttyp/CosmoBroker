using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace CosmoBroker.Services;

public class BrokerConfig
{
    public int Port { get; set; } = 4222;
    public int? ClusterPort { get; set; }
    public List<string> Routes { get; } = new();
}

public static class ConfigParser
{
    public static BrokerConfig Parse(string content)
    {
        var config = new BrokerConfig();
        
        // Very basic recursive-descent style or regex for this simple format
        // port: 4222
        var portMatch = Regex.Match(content, @"port:\s*(\d+)");
        if (portMatch.Success) config.Port = int.Parse(portMatch.Groups[1].Value);

        // cluster { ... }
        var clusterBlock = Regex.Match(content, @"cluster\s*\{([^}]+)\}", RegexOptions.Singleline);
        if (clusterBlock.Success)
        {
            var inner = clusterBlock.Groups[1].Value;
            
            var cPortMatch = Regex.Match(inner, @"port:\s*(\d+)");
            if (cPortMatch.Success) config.ClusterPort = int.Parse(cPortMatch.Groups[1].Value);

            // routes: [ nats://host:port, ... ]
            var routesMatch = Regex.Match(inner, @"routes:\s*\[([^\]]+)\]", RegexOptions.Singleline);
            if (routesMatch.Success)
            {
                var routesStr = routesMatch.Groups[1].Value;
                var parts = routesStr.Split(',', StringSplitOptions.RemoveEmptyEntries);
                foreach (var p in parts)
                {
                    var uri = p.Trim().Trim('"', '\'');
                    if (!string.IsNullOrEmpty(uri)) config.Routes.Add(uri);
                }
            }
        }

        return config;
    }

    public static BrokerConfig LoadFile(string path)
    {
        if (!File.Exists(path)) return new BrokerConfig();
        return Parse(File.ReadAllText(path));
    }
}
