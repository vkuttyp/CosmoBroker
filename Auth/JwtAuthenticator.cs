using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NATS.NKeys;

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
                    // Parse allow/deny pub/sub logic for account-level permissions.
                    if (natsClaim.TryGetProperty("pub", out var pub))
                    {
                        ApplyPerms(pub, account.AllowPublish, account.DenyPublish);
                    }
                    if (natsClaim.TryGetProperty("sub", out var subPerms))
                    {
                        ApplyPerms(subPerms, account.AllowSubscribe, account.DenySubscribe);
                    }
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
                else
                {
                    return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Missing JWT nonce signature" });
                }

                // Verify JWT signature against the issuer (account) public key.
                if (!string.IsNullOrEmpty(iss))
                {
                    bool jwtSigValid = VerifyJwtSignature(options.Jwt, iss);
                    if (!jwtSigValid)
                    {
                        return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Invalid JWT token signature" });
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
        if (string.IsNullOrWhiteSpace(publicKey) || string.IsNullOrWhiteSpace(signature)) return false;
        try
        {
            var kp = KeyPair.FromPublicKey(publicKey);
            var sig = DecodeBase64Any(signature);
            var bytes = Encoding.UTF8.GetBytes(data);
            return kp.Verify(bytes, sig);
        }
        catch
        {
            return false;
        }
    }

    private static void ApplyPerms(JsonElement perms, System.Collections.Generic.List<string> allow, System.Collections.Generic.List<string> deny)
    {
        if (perms.ValueKind != JsonValueKind.Object) return;
        if (perms.TryGetProperty("allow", out var a))
        {
            foreach (var v in ReadStrings(a)) allow.Add(v);
        }
        if (perms.TryGetProperty("deny", out var d))
        {
            foreach (var v in ReadStrings(d)) deny.Add(v);
        }
    }

    private static System.Collections.Generic.IEnumerable<string> ReadStrings(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            var s = element.GetString();
            if (!string.IsNullOrWhiteSpace(s)) yield return s;
            yield break;
        }
        if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String)
                {
                    var s = item.GetString();
                    if (!string.IsNullOrWhiteSpace(s)) yield return s;
                }
            }
        }
    }

    private static byte[] Base64UrlDecode(string input)
    {
        string padded = input.PadRight(input.Length + (4 - input.Length % 4) % 4, '=');
        string base64 = padded.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(base64);
    }

    private static byte[] DecodeBase64Any(string input)
    {
        // Accept both base64 and base64url encodings.
        if (input.Contains('-') || input.Contains('_'))
            return Base64UrlDecode(input);
        return Convert.FromBase64String(input);
    }

    private static bool VerifyJwtSignature(string jwt, string issuerPublicKey)
    {
        try
        {
            var parts = jwt.Split('.');
            if (parts.Length != 3) return false;
            var signingInput = $"{parts[0]}.{parts[1]}";
            var sig = Base64UrlDecode(parts[2]);

            var kp = KeyPair.FromPublicKey(issuerPublicKey);
            return kp.Verify(Encoding.UTF8.GetBytes(signingInput), sig);
        }
        catch
        {
            return false;
        }
    }
}
