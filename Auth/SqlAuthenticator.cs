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
        string? resolvedUser = options.User;
        if (!string.IsNullOrEmpty(options.AuthToken))
        {
            success = await _repo.ValidateTokenAsync(options.AuthToken);
            if (success)
            {
                resolvedUser = await _repo.GetUserByTokenAsync(options.AuthToken) ?? resolvedUser;
            }
        }
        else if (!string.IsNullOrEmpty(options.User))
        {
            success = await _repo.ValidateUserAsync(options.User, options.Pass ?? string.Empty);
        }

        if (!success)
        {
            return new AuthResult { Success = false, ErrorMessage = "SQL Authentication failed" };
        }

        var userName = resolvedUser ?? "sql-user";
        var perms = await _repo.GetUserPermissionsAsync(userName);

        var account = new Account
        {
            Name = perms?.AccountName ?? "sql-account",
            SubjectPrefix = perms?.SubjectPrefix ?? resolvedUser
        };
        if (perms != null)
        {
            account.AllowPublish.AddRange(perms.AllowPublish);
            account.DenyPublish.AddRange(perms.DenyPublish);
            account.AllowSubscribe.AddRange(perms.AllowSubscribe);
            account.DenySubscribe.AddRange(perms.DenySubscribe);
        }

        var user = new User { Name = userName, AccountName = account.Name };

        return new AuthResult { Success = true, Account = account, User = user };
    }
}
