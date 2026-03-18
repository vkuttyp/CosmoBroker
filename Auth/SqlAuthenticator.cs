using System;
using System.Threading.Tasks;
using CosmoBroker.Persistence;

namespace CosmoBroker.Auth;

public class SqlAuthenticator : IAuthenticator
{
    private readonly MessageRepository _repo;

    public SqlAuthenticator(MessageRepository repo)
    {
        _repo = repo ?? throw new ArgumentNullException(nameof(repo));
    }

    public async Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        bool success = false;
        if (!string.IsNullOrEmpty(options.AuthToken))
        {
            success = await _repo.ValidateTokenAsync(options.AuthToken);
        }
        else if (!string.IsNullOrEmpty(options.User))
        {
            success = await _repo.ValidateUserAsync(options.User, options.Pass ?? string.Empty);
        }

        if (!success)
        {
            return new AuthResult { Success = false, ErrorMessage = "SQL Authentication failed" };
        }

        // TODO: Load account and user permissions from the database
        var account = new Account { Name = "sql-account", SubjectPrefix = options.User };
        var user = new User { Name = options.User ?? "sql-user", AccountName = "sql-account" };

        return new AuthResult { Success = true, Account = account, User = user };
    }
}
