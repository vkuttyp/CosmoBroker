using System.Collections.Generic;

namespace CosmoBroker.Auth;

public class Account
{
    public string Name { get; set; } = string.Empty;
    
    // Prefix for total isolation. If null, account shares global space.
    // NATS accounts are isolated by default.
    public string? SubjectPrefix { get; set; }

    // Fine-grained permissions
    public List<string> AllowPublish { get; } = new();
    public List<string> DenyPublish { get; } = new();
    
    public List<string> AllowSubscribe { get; } = new();
    public List<string> DenySubscribe { get; } = new();

    // Mapping for Imports/Exports (advanced)
    // Key: Local subject, Value: Remote (account.subject)
    public Dictionary<string, string> Imports { get; } = new();
    public Dictionary<string, string> Exports { get; } = new();

    public Services.SubjectMapper Mappings { get; } = new();
}

public class User
{
    public string Name { get; set; } = string.Empty;
    public string? AccountName { get; set; }
    
    // Permissions can also be user-level, overriding or adding to account-level
    public List<string> AllowPublish { get; } = new();
    public List<string> AllowSubscribe { get; } = new();
}

public class AuthResult
{
    public bool Success { get; set; }
    public Account? Account { get; set; }
    public User? User { get; set; }
    public string? ErrorMessage { get; set; }
}
