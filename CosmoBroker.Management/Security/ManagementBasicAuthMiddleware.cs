using System.Security.Cryptography;
using System.Text;
using CosmoApiServer.Core.Http;
using CosmoApiServer.Core.Middleware;
using CosmoBroker.Management.Models;

namespace CosmoBroker.Management.Security;

public sealed class ManagementBasicAuthMiddleware(ManagementAuthOptions options) : IMiddleware
{
    public ValueTask InvokeAsync(HttpContext context, RequestDelegate next)
    {
        if (!options.Enabled)
            return next(context);

        if (options.AllowAnonymousHealth && string.Equals(context.Request.Path, "/api/health", StringComparison.OrdinalIgnoreCase))
            return next(context);

        if (TryValidateAuthorization(context.Request.Authorization))
            return next(context);

        context.Response.StatusCode = 401;
        context.Response.Headers["WWW-Authenticate"] = "Basic realm=\"CosmoBroker Management\", charset=\"UTF-8\"";
        context.Response.WriteText("Unauthorized");
        return ValueTask.CompletedTask;
    }

    private bool TryValidateAuthorization(string? authorizationHeader)
    {
        if (string.IsNullOrWhiteSpace(authorizationHeader) ||
            !authorizationHeader.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
            return false;

        string encoded = authorizationHeader["Basic ".Length..].Trim();
        string decoded;

        try
        {
            decoded = Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
        catch (FormatException)
        {
            return false;
        }

        int separatorIndex = decoded.IndexOf(':');
        if (separatorIndex <= 0)
            return false;

        var username = decoded[..separatorIndex];
        var password = decoded[(separatorIndex + 1)..];

        return FixedEquals(username, options.Username) && FixedEquals(password, options.Password);
    }

    private static bool FixedEquals(string left, string right)
    {
        var leftBytes = Encoding.UTF8.GetBytes(left);
        var rightBytes = Encoding.UTF8.GetBytes(right);
        return CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
    }
}
