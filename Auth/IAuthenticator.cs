using System.Threading.Tasks;

namespace CosmoBroker.Auth;

public interface IAuthenticator
{
    /// <summary>
    /// Authenticates a client based on provided options from the CONNECT command.
    /// Returns an AuthResult containing Account and User details if successful.
    /// </summary>
    Task<AuthResult> AuthenticateAsync(ConnectOptions options);
}
