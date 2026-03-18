using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace CosmoBroker.Auth;

/// <summary>
/// Simulates NATS advanced security options (NKEY and JWT).
/// In a real implementation, this would use Ed25519 to verify the signature
/// and validate the JWT chain (Operator -> Account -> User).
/// </summary>
public class JwtAuthenticator : IAuthenticator
{
    // The nonce sent to the client in the INFO block.
    // The client signs this nonce using their NKEY private key.
    private readonly string _serverNonce;

    public JwtAuthenticator(string serverNonce = "secure_nonce_12345")
    {
        _serverNonce = serverNonce;
    }

    public Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        // Check if NKEY auth is used directly
        if (!string.IsNullOrEmpty(options.Nkey) && !string.IsNullOrEmpty(options.Sig))
        {
            // Verify Ed25519 signature of the nonce (Stubbed)
            bool isValid = VerifyEd25519Signature(options.Nkey, options.Sig, _serverNonce);
            if (isValid)
            {
                return Task.FromResult(new AuthResult 
                { 
                    Success = true, 
                    Account = new Account { Name = "nkey-account", SubjectPrefix = null },
                    User = new User { Name = options.Nkey, AccountName = "nkey-account" }
                });
            }
            return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Invalid NKEY signature" });
        }

        // Check if JWT auth is used
        if (!string.IsNullOrEmpty(options.Jwt))
        {
            try
            {
                var parts = options.Jwt.Split('.');
                if (parts.Length != 3) throw new Exception("Invalid JWT format");

                // Parse the payload
                string payloadJson = Encoding.UTF8.GetString(Base64UrlDecode(parts[1]));
                using var doc = JsonDocument.Parse(payloadJson);
                var root = doc.RootElement;

                // NATS JWTs contain standard claims like 'sub' (User NKEY) and 'iss' (Account NKEY)
                string? sub = root.TryGetProperty("sub", out var s) ? s.GetString() : "unknown-user";
                string? iss = root.TryGetProperty("iss", out var i) ? i.GetString() : "unknown-account";
                
                // Advanced NATS JWTs contain a 'nats' claim object with permissions
                Account account = new Account { Name = iss ?? "jwt-account", SubjectPrefix = null };
                User user = new User { Name = sub ?? "jwt-user", AccountName = account.Name };

                if (root.TryGetProperty("nats", out var natsClaim))
                {
                    // Parse allow/deny pub/sub logic here...
                    // Stubbed for simplicity.
                }

                // If JWT is present, the client should still sign the nonce to prove possession of the private key
                if (!string.IsNullOrEmpty(options.Sig))
                {
                    bool isSigValid = VerifyEd25519Signature(sub ?? "", options.Sig, _serverNonce);
                    if (!isSigValid)
                    {
                        return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Invalid JWT signature" });
                    }
                }

                return Task.FromResult(new AuthResult { Success = true, Account = account, User = user });
            }
            catch (Exception ex)
            {
                return Task.FromResult(new AuthResult { Success = false, ErrorMessage = $"JWT parsing failed: {ex.Message}" });
            }
        }

        return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Advanced authentication required (JWT or NKEY)" });
    }

    private bool VerifyEd25519Signature(string publicKey, string signature, string data)
    {
        // STUB: In a real implementation, this uses an Ed25519 library (like NSec)
        // to verify that `signature` is the valid Ed25519 signature of `data` using `publicKey`.
        // For demonstration, we assume it's valid if they are provided.
        return true; 
    }

    private static byte[] Base64UrlDecode(string input)
    {
        string padded = input.PadRight(input.Length + (4 - input.Length % 4) % 4, '=');
        string base64 = padded.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(base64);
    }
}
