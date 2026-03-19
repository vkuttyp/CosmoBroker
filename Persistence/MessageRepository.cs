using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using CosmoSQLClient.Core;
using CosmoSQLClient.Sqlite;
using CosmoSQLClient.MsSql;
using CosmoSQLClient.Postgres;

namespace CosmoBroker.Persistence;

public class MessageRepository
{
    private readonly ISqlDatabase _db;
    private readonly DatabaseProvider _provider;

    public MessageRepository(string connectionString, DatabaseProvider? provider = null, int maxConnections = 5)
    {
        var resolved = ResolveProvider(connectionString, provider);
        _provider = resolved.Provider;
        var cs = resolved.ConnectionString;

        _db = _provider switch
        {
            DatabaseProvider.MsSql => new MsSqlConnectionPool(MsSqlConfiguration.Parse(cs), maxConnections: maxConnections),
            DatabaseProvider.Postgres => new PostgresConnectionPool(PostgresConfiguration.Parse(cs), maxConnections: maxConnections),
            _ => new SqliteConnectionPool(SqliteConfiguration.Parse(cs), maxConnections: maxConnections)
        };
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var schema = _provider switch
        {
            DatabaseProvider.MsSql => MsSqlSchema,
            DatabaseProvider.Postgres => PostgresSchema,
            _ => SqliteSchema
        };

        var statements = schema.Split(';', StringSplitOptions.RemoveEmptyEntries);
        foreach (var stmt in statements)
        {
            await _db.ExecuteAsync(stmt, ct: ct);
        }
    }

    private const string SqliteSchema = @"
            CREATE TABLE IF NOT EXISTS mq_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                subject     TEXT NOT NULL,
                payload     BLOB NOT NULL,
                stream_name TEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS mq_consumers (
                name        TEXT PRIMARY KEY,
                subject     TEXT NOT NULL,
                stream_name TEXT,
                last_msg_id INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS mq_streams (
                name        TEXT PRIMARY KEY,
                subjects    TEXT NOT NULL,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS mq_users (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT NOT NULL UNIQUE,
                password    TEXT NOT NULL,
                token       TEXT
            );
            CREATE TABLE IF NOT EXISTS mq_user_permissions (
                username       TEXT PRIMARY KEY,
                account_name   TEXT,
                subject_prefix TEXT,
                allow_pub      TEXT,
                deny_pub       TEXT,
                allow_sub      TEXT,
                deny_sub       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream ON mq_messages(stream_name);";

    private const string PostgresSchema = @"
            CREATE TABLE IF NOT EXISTS mq_messages (
                id          BIGSERIAL PRIMARY KEY,
                subject     TEXT NOT NULL,
                payload     BYTEA NOT NULL,
                stream_name TEXT,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS mq_consumers (
                name        TEXT PRIMARY KEY,
                subject     TEXT NOT NULL,
                stream_name TEXT,
                last_msg_id BIGINT DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS mq_streams (
                name        TEXT PRIMARY KEY,
                subjects    TEXT NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS mq_users (
                id          BIGSERIAL PRIMARY KEY,
                username    TEXT NOT NULL UNIQUE,
                password    TEXT NOT NULL,
                token       TEXT
            );
            CREATE TABLE IF NOT EXISTS mq_user_permissions (
                username       TEXT PRIMARY KEY,
                account_name   TEXT,
                subject_prefix TEXT,
                allow_pub      TEXT,
                deny_pub       TEXT,
                allow_sub      TEXT,
                deny_sub       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream ON mq_messages(stream_name);";

    private const string MsSqlSchema = @"
            IF OBJECT_ID('mq_messages', 'U') IS NULL
            CREATE TABLE mq_messages (
                id          BIGINT IDENTITY(1,1) PRIMARY KEY,
                subject     NVARCHAR(1024) NOT NULL,
                payload     VARBINARY(MAX) NOT NULL,
                stream_name NVARCHAR(256) NULL,
                created_at  DATETIME2 DEFAULT SYSUTCDATETIME()
            );
            IF OBJECT_ID('mq_consumers', 'U') IS NULL
            CREATE TABLE mq_consumers (
                name        NVARCHAR(256) PRIMARY KEY,
                subject     NVARCHAR(1024) NOT NULL,
                stream_name NVARCHAR(256) NULL,
                last_msg_id BIGINT DEFAULT 0
            );
            IF OBJECT_ID('mq_streams', 'U') IS NULL
            CREATE TABLE mq_streams (
                name        NVARCHAR(256) PRIMARY KEY,
                subjects    NVARCHAR(MAX) NOT NULL,
                created_at  DATETIME2 DEFAULT SYSUTCDATETIME()
            );
            IF OBJECT_ID('mq_users', 'U') IS NULL
            CREATE TABLE mq_users (
                id          BIGINT IDENTITY(1,1) PRIMARY KEY,
                username    NVARCHAR(256) NOT NULL UNIQUE,
                password    NVARCHAR(256) NOT NULL,
                token       NVARCHAR(512) NULL
            );
            IF OBJECT_ID('mq_user_permissions', 'U') IS NULL
            CREATE TABLE mq_user_permissions (
                username       NVARCHAR(256) PRIMARY KEY,
                account_name   NVARCHAR(256) NULL,
                subject_prefix NVARCHAR(256) NULL,
                allow_pub      NVARCHAR(MAX) NULL,
                deny_pub       NVARCHAR(MAX) NULL,
                allow_sub      NVARCHAR(MAX) NULL,
                deny_sub       NVARCHAR(MAX) NULL
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_subject' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_subject ON mq_messages(subject);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_stream' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_stream ON mq_messages(stream_name);";

    public async Task AddUserAsync(string username, string password, string? token = null, CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_users (username, password, token) VALUES (@u, @p, @t) ON CONFLICT(username) DO NOTHING",
            DatabaseProvider.MsSql => "IF NOT EXISTS (SELECT 1 FROM mq_users WHERE username = @u) INSERT INTO mq_users (username, password, token) VALUES (@u, @p, @t)",
            _ => "INSERT OR IGNORE INTO mq_users (username, password, token) VALUES (@u, @p, @t)"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("u", SqlValue.From(username)),
            SqlParameter.Named("p", SqlValue.From(password)),
            SqlParameter.Named("t", SqlValue.From(token ?? string.Empty))
        }, ct: ct);
    }

    public async Task<bool> ValidateUserAsync(string username, string password, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT 1 FROM mq_users WHERE username = @u AND password = @p",
            new[] {
                SqlParameter.Named("u", SqlValue.From(username)),
                SqlParameter.Named("p", SqlValue.From(password))
            }, ct: ct);
        return rows.Count > 0;
    }

    public async Task<bool> ValidateTokenAsync(string token, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT 1 FROM mq_users WHERE token = @t",
            new[] { SqlParameter.Named("t", SqlValue.From(token)) }, ct: ct);
        return rows.Count > 0;
    }

    public async Task<string?> GetUserByTokenAsync(string token, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT username FROM mq_users WHERE token = @t",
            new[] { SqlParameter.Named("t", SqlValue.From(token)) }, ct: ct);
        if (rows.Count == 0) return null;
        return rows[0]["username"].AsString();
    }

    public async Task<UserPermissions?> GetUserPermissionsAsync(string username, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT account_name, subject_prefix, allow_pub, deny_pub, allow_sub, deny_sub FROM mq_user_permissions WHERE username = @u",
            new[] { SqlParameter.Named("u", SqlValue.From(username)) }, ct: ct);
        if (rows.Count == 0) return null;

        var row = rows[0];
        return new UserPermissions
        {
            Username = username,
            AccountName = row["account_name"].AsString(),
            SubjectPrefix = row["subject_prefix"].AsString(),
            AllowPublish = SplitCsv(row["allow_pub"].AsString()),
            DenyPublish = SplitCsv(row["deny_pub"].AsString()),
            AllowSubscribe = SplitCsv(row["allow_sub"].AsString()),
            DenySubscribe = SplitCsv(row["deny_sub"].AsString())
        };
    }

    public async Task<long> SaveMessageAsync(string subject, byte[] payload, CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_messages (subject, payload) VALUES (@subject, @payload) RETURNING id",
            DatabaseProvider.MsSql => "INSERT INTO mq_messages (subject, payload) VALUES (@subject, @payload); SELECT CAST(SCOPE_IDENTITY() AS BIGINT) as id;",
            _ => "INSERT INTO mq_messages (subject, payload) VALUES (@subject, @payload); SELECT last_insert_rowid() as id;"
        };

        var rows = await _db.QueryAsync(sql, new[] {
            SqlParameter.Named("subject", SqlValue.From(subject)),
            SqlParameter.Named("payload", SqlValue.From(payload))
        }, ct: ct);

        return rows[0]["id"].AsInt() ?? 0;
    }

    public async Task<List<PersistedMessage>> GetMessagesAsync(string subject, long startAfterId, int limit = 100, CancellationToken ct = default)
    {
        var query = _provider == DatabaseProvider.MsSql
            ? "SELECT TOP (@limit) id, subject, payload FROM mq_messages WHERE subject = @subject AND id > @id ORDER BY id ASC"
            : "SELECT id, subject, payload FROM mq_messages WHERE subject = @subject AND id > @id ORDER BY id ASC LIMIT @limit";
        var rows = await _db.QueryAsync(query, new[] {
            SqlParameter.Named("subject", SqlValue.From(subject)),
            SqlParameter.Named("id", SqlValue.From(startAfterId)),
            SqlParameter.Named("limit", SqlValue.From(limit))
        }, ct: ct);

        var list = new List<PersistedMessage>();
        foreach (var row in rows)
        {
            list.Add(new PersistedMessage {
                Id = row["id"].AsInt() ?? 0,
                Subject = row["subject"].AsString() ?? string.Empty,
                Payload = row["payload"].AsBytes() ?? Array.Empty<byte>()
            });
        }
        return list;
    }

    public async Task<long> GetConsumerOffsetAsync(string name, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT last_msg_id FROM mq_consumers WHERE name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(name)) }, ct: ct);
        
        if (rows.Count == 0) return 0;
        return rows[0]["last_msg_id"].AsInt() ?? 0;
    }

    public async Task UpdateConsumerOffsetAsync(string name, string subject, long lastId, CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_consumers (name, subject, last_msg_id) VALUES (@name, @subject, @id) " +
                                         "ON CONFLICT(name) DO UPDATE SET last_msg_id = EXCLUDED.last_msg_id, subject = EXCLUDED.subject",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM mq_consumers WHERE name = @name) " +
                                      "UPDATE mq_consumers SET last_msg_id = @id, subject = @subject WHERE name = @name " +
                                      "ELSE INSERT INTO mq_consumers (name, subject, last_msg_id) VALUES (@name, @subject, @id)",
            _ => "INSERT INTO mq_consumers (name, subject, last_msg_id) VALUES (@name, @subject, @id) " +
                 "ON CONFLICT(name) DO UPDATE SET last_msg_id = excluded.last_msg_id"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("name", SqlValue.From(name)),
            SqlParameter.Named("subject", SqlValue.From(subject)),
            SqlParameter.Named("id", SqlValue.From(lastId))
        }, ct: ct);
    }

    // JetStream Persistence
    public async Task SaveStreamAsync(string name, string subjects, CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_streams (name, subjects) VALUES (@name, @subjects) " +
                                         "ON CONFLICT(name) DO UPDATE SET subjects = EXCLUDED.subjects",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM mq_streams WHERE name = @name) " +
                                      "UPDATE mq_streams SET subjects = @subjects WHERE name = @name " +
                                      "ELSE INSERT INTO mq_streams (name, subjects) VALUES (@name, @subjects)",
            _ => "INSERT INTO mq_streams (name, subjects) VALUES (@name, @subjects) " +
                 "ON CONFLICT(name) DO UPDATE SET subjects = excluded.subjects"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("name", SqlValue.From(name)),
            SqlParameter.Named("subjects", SqlValue.From(subjects))
        }, ct: ct);
    }

    public async Task<List<PersistedStream>> GetStreamsAsync(CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT name, subjects FROM mq_streams", ct: ct);
        return rows.Select(r => new PersistedStream {
            Name = r["name"].AsString() ?? string.Empty,
            Subjects = r["subjects"].AsString() ?? string.Empty
        }).ToList();
    }

    public async Task<long> SaveJetStreamMessageAsync(string streamName, string subject, byte[] payload, CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_messages (subject, payload, stream_name) VALUES (@subject, @payload, @stream) RETURNING id",
            DatabaseProvider.MsSql => "INSERT INTO mq_messages (subject, payload, stream_name) VALUES (@subject, @payload, @stream); SELECT CAST(SCOPE_IDENTITY() AS BIGINT) as id;",
            _ => "INSERT INTO mq_messages (subject, payload, stream_name) VALUES (@subject, @payload, @stream); SELECT last_insert_rowid() as id;"
        };

        var rows = await _db.QueryAsync(sql, new[] {
            SqlParameter.Named("subject", SqlValue.From(subject)),
            SqlParameter.Named("payload", SqlValue.From(payload)),
            SqlParameter.Named("stream", SqlValue.From(streamName))
        }, ct: ct);

        return rows[0]["id"].AsInt() ?? 0;
    }

    public async Task<List<PersistedMessage>> GetJetStreamMessagesAsync(string streamName, long startAfterId, int limit = 100, CancellationToken ct = default)
    {
        var query = _provider == DatabaseProvider.MsSql
            ? "SELECT TOP (@limit) id, subject, payload FROM mq_messages WHERE stream_name = @stream AND id > @id ORDER BY id ASC"
            : "SELECT id, subject, payload FROM mq_messages WHERE stream_name = @stream AND id > @id ORDER BY id ASC LIMIT @limit";
        var rows = await _db.QueryAsync(query, new[] {
            SqlParameter.Named("stream", SqlValue.From(streamName)),
            SqlParameter.Named("id", SqlValue.From(startAfterId)),
            SqlParameter.Named("limit", SqlValue.From(limit))
        }, ct: ct);

        return rows.Select(row => new PersistedMessage {
            Id = row["id"].AsInt() ?? 0,
            Subject = row["subject"].AsString() ?? string.Empty,
            Payload = row["payload"].AsBytes() ?? Array.Empty<byte>()
        }).ToList();
    }

    private static List<string> SplitCsv(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv)) return new List<string>();
        return csv.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
    }

    private static (DatabaseProvider Provider, string ConnectionString) ResolveProvider(string connectionString, DatabaseProvider? provider)
    {
        if (provider.HasValue) return (provider.Value, connectionString);

        string cs = connectionString;
        if (cs.StartsWith("sqlite:", StringComparison.OrdinalIgnoreCase))
            return (DatabaseProvider.Sqlite, cs.Substring("sqlite:".Length));
        if (cs.StartsWith("postgres:", StringComparison.OrdinalIgnoreCase) || cs.StartsWith("postgresql:", StringComparison.OrdinalIgnoreCase) || cs.StartsWith("pg:", StringComparison.OrdinalIgnoreCase))
            return (DatabaseProvider.Postgres, cs.Substring(cs.IndexOf(':') + 1));
        if (cs.StartsWith("mssql:", StringComparison.OrdinalIgnoreCase) || cs.StartsWith("sqlserver:", StringComparison.OrdinalIgnoreCase))
            return (DatabaseProvider.MsSql, cs.Substring(cs.IndexOf(':') + 1));

        bool looksSqlite = cs.Contains("Data Source=", StringComparison.OrdinalIgnoreCase) &&
                           (cs.Contains(".db", StringComparison.OrdinalIgnoreCase) ||
                            cs.Contains(".sqlite", StringComparison.OrdinalIgnoreCase) ||
                            cs.Contains("Mode=", StringComparison.OrdinalIgnoreCase) ||
                            cs.Contains("Cache=", StringComparison.OrdinalIgnoreCase));
        if (looksSqlite) return (DatabaseProvider.Sqlite, cs);

        bool looksPostgres = cs.Contains("Host=", StringComparison.OrdinalIgnoreCase) &&
                             (cs.Contains("Username=", StringComparison.OrdinalIgnoreCase) || cs.Contains("User Id=", StringComparison.OrdinalIgnoreCase));
        if (looksPostgres) return (DatabaseProvider.Postgres, cs);

        bool looksMsSql = cs.Contains("Server=", StringComparison.OrdinalIgnoreCase) ||
                          cs.Contains("Initial Catalog=", StringComparison.OrdinalIgnoreCase) ||
                          (cs.Contains("Data Source=", StringComparison.OrdinalIgnoreCase) && !looksSqlite);
        if (looksMsSql) return (DatabaseProvider.MsSql, cs);

        return (DatabaseProvider.Sqlite, cs);
    }
}

public enum DatabaseProvider
{
    Sqlite,
    Postgres,
    MsSql
}

public class UserPermissions
{
    public required string Username { get; set; }
    public string? AccountName { get; set; }
    public string? SubjectPrefix { get; set; }
    public List<string> AllowPublish { get; set; } = new();
    public List<string> DenyPublish { get; set; } = new();
    public List<string> AllowSubscribe { get; set; } = new();
    public List<string> DenySubscribe { get; set; } = new();
}

public class PersistedStream
{
    public required string Name { get; set; }
    public required string Subjects { get; set; }
}

public class PersistedMessage
{
    public long Id { get; set; }
    public required string Subject { get; set; }
    public required byte[] Payload { get; set; }
}
