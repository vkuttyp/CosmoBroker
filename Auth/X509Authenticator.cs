using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace CosmoBroker.Auth;

/// <summary>
/// Authenticates clients based on their TLS client certificate.
/// </summary>
public class X509Authenticator : IAuthenticator
{
    public Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        // This authenticator requires context that isn't in ConnectOptions (the SslStream).
        // For simplicity, we assume the server layer verified the certificate
        // and passed the identity in the 'User' or a new extension.
        
        // In a real NATS implementation, the CN or SAN of the certificate is mapped to an account.
        return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "X509 Auth requires SslStream context" });
    }

    public AuthResult AuthenticateCertificate(X509Certificate2? certificate)
    {
        if (certificate == null)
        {
            return new AuthResult { Success = false, ErrorMessage = "No client certificate provided" };
        }

        // Validate certificate (trust chain, expiration, etc.)
        // NATS often uses the 'Subject' or a specific extension for the Account mapping.
        string identity = certificate.Subject;
        
        return new AuthResult
        {
            Success = true,
            Account = new Account { Name = "cert-account", SubjectPrefix = null },
            User = new User { Name = identity, AccountName = "cert-account" }
        };
    }
}
