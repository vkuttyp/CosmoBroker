using System.Threading.Tasks;

namespace CosmoBroker.Auth;

public class SimpleAuthenticator : IAuthenticator
{
    private readonly string? _user;
    private readonly string? _pass;
    private readonly string? _token;

    public SimpleAuthenticator(string? user = null, string? pass = null, string? token = null)
    {
        _user = user;
        _pass = pass;
        _token = token;
    }

    public Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        bool success = false;
        string? user = options.User;

        if (!string.IsNullOrEmpty(_token))
        {
            success = options.AuthToken == _token;
        }
        else if (!string.IsNullOrEmpty(_user))
        {
            success = options.User == _user && options.Pass == _pass;
        }
        else
        {
            success = true; // No auth required
        }

        if (!success)
        {
            return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "Authentication failed" });
        }

        // Return a default account and user if successful
        var account = new Account { Name = "default", SubjectPrefix = null };
        var authUser = new User { Name = user ?? "anonymous", AccountName = "default" };

        return Task.FromResult(new AuthResult { Success = true, Account = account, User = authUser });
    }
}
