using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CosmoSQLClient.Core;
using CosmoSQLClient.Sqlite;
using CosmoSQLClient.MsSql;
using CosmoSQLClient.Postgres;
using CosmoBroker.RabbitMQ;

namespace CosmoBroker.Persistence;

public class MessageRepository
{
    private readonly ISqlDatabase _db;
    private readonly DatabaseProvider _provider;
    private readonly Channel<JetStreamWrite>? _jsWriteChannel;
    private readonly Task? _jsWriteLoop;
    private readonly Channel<RabbitWrite>? _rmqWriteChannel;
    private readonly Task? _rmqWriteLoop;
    private readonly Channel<RabbitDelete>? _rmqDeleteChannel;
    private readonly Task? _rmqDeleteLoop;
    private readonly SemaphoreSlim _sqliteRabbitWriteLock = new(1, 1);

    private readonly int _jsBatchSize;
    private readonly TimeSpan _jsBatchMaxDelay;
    private readonly int _rmqBatchSize;
    private readonly TimeSpan _rmqBatchMaxDelay;

    public MessageRepository(
        string connectionString,
        DatabaseProvider? provider = null,
        int maxConnections = 5,
        int? jetStreamBatchSize = null,
        int? jetStreamBatchDelayMs = null)
    {
        var resolved = ResolveProvider(connectionString, provider);
        _provider = resolved.Provider;
        var cs = resolved.ConnectionString;
        int effectiveMaxConnections = _provider == DatabaseProvider.Sqlite ? 1 : maxConnections;

        _jsBatchSize = ResolveJetStreamBatchSize(jetStreamBatchSize);
        _jsBatchMaxDelay = ResolveJetStreamBatchDelay(jetStreamBatchDelayMs);
        _rmqBatchSize = ResolveRabbitBatchSize();
        _rmqBatchMaxDelay = ResolveRabbitBatchDelay();

        _db = _provider switch
        {
            DatabaseProvider.MsSql => new MsSqlConnectionPool(MsSqlConfiguration.Parse(cs), maxConnections: effectiveMaxConnections),
            DatabaseProvider.Postgres => new PostgresConnectionPool(PostgresConfiguration.Parse(cs), maxConnections: effectiveMaxConnections),
            _ => new SqliteConnectionPool(SqliteConfiguration.Parse(cs), maxConnections: effectiveMaxConnections)
        };

        if (_provider == DatabaseProvider.Sqlite)
        {
            _jsWriteChannel = Channel.CreateUnbounded<JetStreamWrite>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            _jsWriteLoop = Task.Run(JetStreamWriteLoopAsync);

            _rmqWriteChannel = Channel.CreateUnbounded<RabbitWrite>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            _rmqWriteLoop = Task.Run(RabbitWriteLoopAsync);

            _rmqDeleteChannel = Channel.CreateUnbounded<RabbitDelete>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            _rmqDeleteLoop = Task.Run(RabbitDeleteLoopAsync);
        }
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        if (_provider == DatabaseProvider.Sqlite)
        {
            await ApplySqlitePragmasAsync(ct);
        }

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

        await EnsureRabbitMessagePropertyColumnAsync(ct);
        await EnsureRabbitBindingDestinationKindColumnAsync(ct);
        await EnsureRabbitQueueTypeColumnAsync(ct);
        await EnsureRabbitExchangeMetadataColumnsAsync(ct);
        await EnsureRabbitStreamRetentionColumnsAsync(ct);
        await EnsureRabbitStreamPublisherTableAsync(ct);
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
            CREATE TABLE IF NOT EXISTS mq_rabbit_permissions (
                username            TEXT PRIMARY KEY,
                default_vhost       TEXT,
                allowed_vhosts      TEXT,
                configure_patterns  TEXT,
                write_patterns      TEXT,
                read_patterns       TEXT
            );
            CREATE TABLE IF NOT EXISTS rmq_exchanges (
                name        TEXT PRIMARY KEY,
                type        TEXT NOT NULL,
                durable     INTEGER NOT NULL,
                auto_delete INTEGER NOT NULL,
                super_stream_partitions INTEGER
            );
            CREATE TABLE IF NOT EXISTS rmq_queues (
                name                    TEXT PRIMARY KEY,
                queue_type              TEXT NOT NULL DEFAULT 'classic',
                durable                 INTEGER NOT NULL,
                exclusive               INTEGER NOT NULL,
                auto_delete             INTEGER NOT NULL,
                max_priority            INTEGER NOT NULL,
                dead_letter_exchange    TEXT,
                dead_letter_routing_key TEXT,
                message_ttl_ms          INTEGER,
                queue_ttl_ms            INTEGER,
                stream_max_length_messages INTEGER,
                stream_max_length_bytes INTEGER,
                stream_max_age_ms       INTEGER
            );
            CREATE TABLE IF NOT EXISTS rmq_bindings (
                exchange_name TEXT NOT NULL,
                queue_name    TEXT NOT NULL,
                routing_key   TEXT NOT NULL,
                destination_kind TEXT NOT NULL DEFAULT 'queue',
                header_args   TEXT,
                PRIMARY KEY (exchange_name, queue_name, routing_key)
            );
            CREATE TABLE IF NOT EXISTS rmq_stream_consumers (
                queue_name    TEXT NOT NULL,
                consumer_tag  TEXT NOT NULL,
                last_offset   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (queue_name, consumer_tag)
            );
            CREATE TABLE IF NOT EXISTS rmq_stream_publishers (
                queue_name    TEXT NOT NULL,
                publisher_ref TEXT NOT NULL,
                last_sequence INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (queue_name, publisher_ref)
            );
            CREATE TABLE IF NOT EXISTS rmq_messages (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name            TEXT NOT NULL,
                routing_key           TEXT NOT NULL,
                payload               BLOB NOT NULL,
                headers               TEXT,
                properties            TEXT,
                priority              INTEGER NOT NULL,
                expires_at            TEXT,
                death_count           INTEGER NOT NULL,
                original_exchange     TEXT,
                original_routing_key  TEXT,
                created_at            DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream ON mq_messages(stream_name);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream_id ON mq_messages(stream_name, id);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject_id ON mq_messages(subject, id);
            CREATE INDEX IF NOT EXISTS idx_rmq_messages_queue_id ON rmq_messages(queue_name, id);";

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
            CREATE TABLE IF NOT EXISTS mq_rabbit_permissions (
                username            TEXT PRIMARY KEY,
                default_vhost       TEXT,
                allowed_vhosts      TEXT,
                configure_patterns  TEXT,
                write_patterns      TEXT,
                read_patterns       TEXT
            );
            CREATE TABLE IF NOT EXISTS rmq_exchanges (
                name        TEXT PRIMARY KEY,
                type        TEXT NOT NULL,
                durable     BOOLEAN NOT NULL,
                auto_delete BOOLEAN NOT NULL,
                super_stream_partitions INTEGER
            );
            CREATE TABLE IF NOT EXISTS rmq_queues (
                name                    TEXT PRIMARY KEY,
                queue_type              TEXT NOT NULL DEFAULT 'classic',
                durable                 BOOLEAN NOT NULL,
                exclusive               BOOLEAN NOT NULL,
                auto_delete             BOOLEAN NOT NULL,
                max_priority            INTEGER NOT NULL,
                dead_letter_exchange    TEXT,
                dead_letter_routing_key TEXT,
                message_ttl_ms          INTEGER,
                queue_ttl_ms            INTEGER,
                stream_max_length_messages BIGINT,
                stream_max_length_bytes BIGINT,
                stream_max_age_ms       BIGINT
            );
            CREATE TABLE IF NOT EXISTS rmq_bindings (
                exchange_name TEXT NOT NULL,
                queue_name    TEXT NOT NULL,
                routing_key   TEXT NOT NULL,
                destination_kind TEXT NOT NULL DEFAULT 'queue',
                header_args   TEXT,
                PRIMARY KEY (exchange_name, queue_name, routing_key)
            );
            CREATE TABLE IF NOT EXISTS rmq_stream_consumers (
                queue_name    TEXT NOT NULL,
                consumer_tag  TEXT NOT NULL,
                last_offset   BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue_name, consumer_tag)
            );
            CREATE TABLE IF NOT EXISTS rmq_stream_publishers (
                queue_name    TEXT NOT NULL,
                publisher_ref TEXT NOT NULL,
                last_sequence BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue_name, publisher_ref)
            );
            CREATE TABLE IF NOT EXISTS rmq_messages (
                id                    BIGSERIAL PRIMARY KEY,
                queue_name            TEXT NOT NULL,
                routing_key           TEXT NOT NULL,
                payload               BYTEA NOT NULL,
                headers               TEXT,
                properties            TEXT,
                priority              INTEGER NOT NULL,
                expires_at            TIMESTAMPTZ,
                death_count           INTEGER NOT NULL,
                original_exchange     TEXT,
                original_routing_key  TEXT,
                created_at            TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream ON mq_messages(stream_name);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_stream_id ON mq_messages(stream_name, id);
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject_id ON mq_messages(subject, id);
            CREATE INDEX IF NOT EXISTS idx_rmq_messages_queue_id ON rmq_messages(queue_name, id);";

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
            IF OBJECT_ID('mq_rabbit_permissions', 'U') IS NULL
            CREATE TABLE mq_rabbit_permissions (
                username           NVARCHAR(256) PRIMARY KEY,
                default_vhost      NVARCHAR(256) NULL,
                allowed_vhosts     NVARCHAR(MAX) NULL,
                configure_patterns NVARCHAR(MAX) NULL,
                write_patterns     NVARCHAR(MAX) NULL,
                read_patterns      NVARCHAR(MAX) NULL
            );
            IF OBJECT_ID('rmq_exchanges', 'U') IS NULL
            CREATE TABLE rmq_exchanges (
                name        NVARCHAR(256) PRIMARY KEY,
                type        NVARCHAR(64) NOT NULL,
                durable     BIT NOT NULL,
                auto_delete BIT NOT NULL,
                super_stream_partitions INT NULL
            );
            IF OBJECT_ID('rmq_queues', 'U') IS NULL
            CREATE TABLE rmq_queues (
                name                    NVARCHAR(256) PRIMARY KEY,
                queue_type              NVARCHAR(32) NOT NULL CONSTRAINT DF_rmq_queues_queue_type DEFAULT 'classic',
                durable                 BIT NOT NULL,
                exclusive               BIT NOT NULL,
                auto_delete             BIT NOT NULL,
                max_priority            TINYINT NOT NULL,
                dead_letter_exchange    NVARCHAR(256) NULL,
                dead_letter_routing_key NVARCHAR(256) NULL,
                message_ttl_ms          INT NULL,
                queue_ttl_ms            INT NULL,
                stream_max_length_messages BIGINT NULL,
                stream_max_length_bytes BIGINT NULL,
                stream_max_age_ms       BIGINT NULL
            );
            IF OBJECT_ID('rmq_bindings', 'U') IS NULL
            CREATE TABLE rmq_bindings (
                exchange_name NVARCHAR(256) NOT NULL,
                queue_name    NVARCHAR(256) NOT NULL,
                routing_key   NVARCHAR(512) NOT NULL,
                destination_kind NVARCHAR(32) NOT NULL DEFAULT 'queue',
                header_args   NVARCHAR(MAX) NULL,
                CONSTRAINT PK_rmq_bindings PRIMARY KEY (exchange_name, queue_name, routing_key)
            );
            IF OBJECT_ID('rmq_stream_consumers', 'U') IS NULL
            CREATE TABLE rmq_stream_consumers (
                queue_name NVARCHAR(256) NOT NULL,
                consumer_tag NVARCHAR(256) NOT NULL,
                last_offset BIGINT NOT NULL DEFAULT 0,
                CONSTRAINT PK_rmq_stream_consumers PRIMARY KEY (queue_name, consumer_tag)
            );
            IF OBJECT_ID('rmq_stream_publishers', 'U') IS NULL
            CREATE TABLE rmq_stream_publishers (
                queue_name NVARCHAR(256) NOT NULL,
                publisher_ref NVARCHAR(256) NOT NULL,
                last_sequence BIGINT NOT NULL DEFAULT 0,
                CONSTRAINT PK_rmq_stream_publishers PRIMARY KEY (queue_name, publisher_ref)
            );
            IF OBJECT_ID('rmq_messages', 'U') IS NULL
            CREATE TABLE rmq_messages (
                id                    BIGINT IDENTITY(1,1) PRIMARY KEY,
                queue_name            NVARCHAR(256) NOT NULL,
                routing_key           NVARCHAR(512) NOT NULL,
                payload               VARBINARY(MAX) NOT NULL,
                headers               NVARCHAR(MAX) NULL,
                properties            NVARCHAR(MAX) NULL,
                priority              TINYINT NOT NULL,
                expires_at            DATETIME2 NULL,
                death_count           INT NOT NULL,
                original_exchange     NVARCHAR(256) NULL,
                original_routing_key  NVARCHAR(512) NULL,
                created_at            DATETIME2 DEFAULT SYSUTCDATETIME()
            );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_subject' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_subject ON mq_messages(subject);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_stream' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_stream ON mq_messages(stream_name);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_stream_id' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_stream_id ON mq_messages(stream_name, id);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_mq_messages_subject_id' AND object_id = OBJECT_ID('mq_messages'))
            CREATE INDEX idx_mq_messages_subject_id ON mq_messages(subject, id);
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_rmq_messages_queue_id' AND object_id = OBJECT_ID('rmq_messages'))
            CREATE INDEX idx_rmq_messages_queue_id ON rmq_messages(queue_name, id);";

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

    public async Task SaveRabbitPermissionsAsync(
        string username,
        string? defaultVhost,
        IReadOnlyCollection<string>? allowedVhosts,
        IReadOnlyCollection<string>? configurePatterns,
        IReadOnlyCollection<string>? writePatterns,
        IReadOnlyCollection<string>? readPatterns,
        CancellationToken ct = default)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO mq_rabbit_permissions (username, default_vhost, allowed_vhosts, configure_patterns, write_patterns, read_patterns) " +
                                         "VALUES (@username, @defaultVhost, @allowedVhosts, @configurePatterns, @writePatterns, @readPatterns) " +
                                         "ON CONFLICT(username) DO UPDATE SET default_vhost = EXCLUDED.default_vhost, allowed_vhosts = EXCLUDED.allowed_vhosts, configure_patterns = EXCLUDED.configure_patterns, write_patterns = EXCLUDED.write_patterns, read_patterns = EXCLUDED.read_patterns",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM mq_rabbit_permissions WHERE username = @username) " +
                                      "UPDATE mq_rabbit_permissions SET default_vhost = @defaultVhost, allowed_vhosts = @allowedVhosts, configure_patterns = @configurePatterns, write_patterns = @writePatterns, read_patterns = @readPatterns WHERE username = @username " +
                                      "ELSE INSERT INTO mq_rabbit_permissions (username, default_vhost, allowed_vhosts, configure_patterns, write_patterns, read_patterns) VALUES (@username, @defaultVhost, @allowedVhosts, @configurePatterns, @writePatterns, @readPatterns)",
            _ => "INSERT INTO mq_rabbit_permissions (username, default_vhost, allowed_vhosts, configure_patterns, write_patterns, read_patterns) " +
                 "VALUES (@username, @defaultVhost, @allowedVhosts, @configurePatterns, @writePatterns, @readPatterns) " +
                 "ON CONFLICT(username) DO UPDATE SET default_vhost = excluded.default_vhost, allowed_vhosts = excluded.allowed_vhosts, configure_patterns = excluded.configure_patterns, write_patterns = excluded.write_patterns, read_patterns = excluded.read_patterns"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("username", SqlValue.From(username)),
            SqlParameter.Named("defaultVhost", SqlValue.From(defaultVhost ?? "/")),
            SqlParameter.Named("allowedVhosts", SqlValue.From(JoinCsv(allowedVhosts))),
            SqlParameter.Named("configurePatterns", SqlValue.From(JoinCsv(configurePatterns))),
            SqlParameter.Named("writePatterns", SqlValue.From(JoinCsv(writePatterns))),
            SqlParameter.Named("readPatterns", SqlValue.From(JoinCsv(readPatterns)))
        }, ct: ct);
    }

    public async Task<RabbitPermissions?> GetRabbitPermissionsAsync(string username, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT default_vhost, allowed_vhosts, configure_patterns, write_patterns, read_patterns FROM mq_rabbit_permissions WHERE username = @u",
            new[] { SqlParameter.Named("u", SqlValue.From(username)) }, ct: ct);
        if (rows.Count == 0) return null;

        var row = rows[0];
        return new RabbitPermissions
        {
            Username = username,
            DefaultVhost = NullIfEmpty(row["default_vhost"].AsString()) ?? "/",
            AllowedVhosts = SplitCsv(row["allowed_vhosts"].AsString()),
            ConfigurePatterns = SplitCsv(row["configure_patterns"].AsString()),
            WritePatterns = SplitCsv(row["write_patterns"].AsString()),
            ReadPatterns = SplitCsv(row["read_patterns"].AsString())
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

    public async Task<long?> GetRabbitStreamConsumerOffsetAsync(string vhost, string queueName, string consumerTag, CancellationToken ct = default)
    {
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        var rows = await _db.QueryAsync(
            "SELECT last_offset FROM rmq_stream_consumers WHERE queue_name = @queue AND consumer_tag = @consumer",
            new[]
            {
                SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
                SqlParameter.Named("consumer", SqlValue.From(consumerTag))
            },
            ct: ct);

        if (rows.Count == 0)
            return null;

        return rows[0]["last_offset"].AsInt();
    }

    public async Task UpdateRabbitStreamConsumerOffsetAsync(string vhost, string queueName, string consumerTag, long lastOffset, CancellationToken ct = default)
    {
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_stream_consumers (queue_name, consumer_tag, last_offset) VALUES (@queue, @consumer, @offset) " +
                                         "ON CONFLICT(queue_name, consumer_tag) DO UPDATE SET last_offset = EXCLUDED.last_offset",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM rmq_stream_consumers WHERE queue_name = @queue AND consumer_tag = @consumer) " +
                                      "UPDATE rmq_stream_consumers SET last_offset = @offset WHERE queue_name = @queue AND consumer_tag = @consumer " +
                                      "ELSE INSERT INTO rmq_stream_consumers (queue_name, consumer_tag, last_offset) VALUES (@queue, @consumer, @offset)",
            _ => "INSERT INTO rmq_stream_consumers (queue_name, consumer_tag, last_offset) VALUES (@queue, @consumer, @offset) " +
                 "ON CONFLICT(queue_name, consumer_tag) DO UPDATE SET last_offset = excluded.last_offset"
        };

        await _db.ExecuteAsync(sql, new[]
        {
            SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
            SqlParameter.Named("consumer", SqlValue.From(consumerTag)),
            SqlParameter.Named("offset", SqlValue.From(lastOffset))
        }, ct: ct);
    }

    public async Task<ulong?> GetRabbitStreamPublisherSequenceAsync(string vhost, string queueName, string publisherRef, CancellationToken ct = default)
    {
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        var rows = await _db.QueryAsync(
            "SELECT last_sequence FROM rmq_stream_publishers WHERE queue_name = @queue AND publisher_ref = @publisher",
            new[]
            {
                SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
                SqlParameter.Named("publisher", SqlValue.From(publisherRef))
            },
            ct: ct);

        if (rows.Count == 0)
            return null;

        return (ulong)(rows[0]["last_sequence"].AsInt() ?? 0);
    }

    public async Task UpdateRabbitStreamPublisherSequenceAsync(string vhost, string queueName, string publisherRef, ulong lastSequence, CancellationToken ct = default)
    {
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_stream_publishers (queue_name, publisher_ref, last_sequence) VALUES (@queue, @publisher, @sequence) " +
                                         "ON CONFLICT(queue_name, publisher_ref) DO UPDATE SET last_sequence = EXCLUDED.last_sequence",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM rmq_stream_publishers WHERE queue_name = @queue AND publisher_ref = @publisher) " +
                                      "UPDATE rmq_stream_publishers SET last_sequence = @sequence WHERE queue_name = @queue AND publisher_ref = @publisher " +
                                      "ELSE INSERT INTO rmq_stream_publishers (queue_name, publisher_ref, last_sequence) VALUES (@queue, @publisher, @sequence)",
            _ => "INSERT INTO rmq_stream_publishers (queue_name, publisher_ref, last_sequence) VALUES (@queue, @publisher, @sequence) " +
                 "ON CONFLICT(queue_name, publisher_ref) DO UPDATE SET last_sequence = excluded.last_sequence"
        };

        await _db.ExecuteAsync(sql, new[]
        {
            SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
            SqlParameter.Named("publisher", SqlValue.From(publisherRef)),
            SqlParameter.Named("sequence", SqlValue.From((long)lastSequence))
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
        if (_provider == DatabaseProvider.Sqlite && _jsWriteChannel != null)
        {
            if (ct.IsCancellationRequested) return await Task.FromCanceled<long>(ct);
            var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            var write = new JetStreamWrite(streamName, subject, payload, tcs);
            try
            {
                await _jsWriteChannel.Writer.WriteAsync(write, ct);
            }
            catch (OperationCanceledException)
            {
                tcs.TrySetCanceled(ct);
            }
            return await tcs.Task;
        }

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

    private async Task ApplySqlitePragmasAsync(CancellationToken ct)
    {
        // Safe-profile pragmas: durable, WAL-based, and tuned for throughput.
        await _db.ExecuteAsync("PRAGMA journal_mode=WAL;", ct: ct);
        await _db.ExecuteAsync("PRAGMA synchronous=FULL;", ct: ct);
        await _db.ExecuteAsync("PRAGMA temp_store=MEMORY;", ct: ct);
        await _db.ExecuteAsync("PRAGMA cache_size=-200000;", ct: ct);
        await _db.ExecuteAsync("PRAGMA mmap_size=268435456;", ct: ct);
        await _db.ExecuteAsync("PRAGMA busy_timeout=5000;", ct: ct);
    }

    private Task EnsureRabbitMessagePropertyColumnAsync(CancellationToken ct)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_messages ADD COLUMN IF NOT EXISTS properties TEXT",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_messages', 'properties') IS NULL ALTER TABLE rmq_messages ADD properties NVARCHAR(MAX) NULL",
            _ => "ALTER TABLE rmq_messages ADD COLUMN properties TEXT"
        };

        return ExecuteIgnoreDuplicateColumnAsync(sql, ct);
    }

    private Task EnsureRabbitBindingDestinationKindColumnAsync(CancellationToken ct)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_bindings ADD COLUMN IF NOT EXISTS destination_kind TEXT NOT NULL DEFAULT 'queue'",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_bindings', 'destination_kind') IS NULL ALTER TABLE rmq_bindings ADD destination_kind NVARCHAR(32) NOT NULL CONSTRAINT DF_rmq_bindings_destination_kind DEFAULT 'queue'",
            _ => "ALTER TABLE rmq_bindings ADD COLUMN destination_kind TEXT NOT NULL DEFAULT 'queue'"
        };

        return ExecuteIgnoreDuplicateColumnAsync(sql, ct);
    }

    private Task EnsureRabbitQueueTypeColumnAsync(CancellationToken ct)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_queues ADD COLUMN IF NOT EXISTS queue_type TEXT NOT NULL DEFAULT 'classic'",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_queues', 'queue_type') IS NULL ALTER TABLE rmq_queues ADD queue_type NVARCHAR(32) NOT NULL CONSTRAINT DF_rmq_queues_queue_type_compat DEFAULT 'classic'",
            _ => "ALTER TABLE rmq_queues ADD COLUMN queue_type TEXT NOT NULL DEFAULT 'classic'"
        };

        return ExecuteIgnoreDuplicateColumnAsync(sql, ct);
    }

    private Task EnsureRabbitExchangeMetadataColumnsAsync(CancellationToken ct)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_exchanges ADD COLUMN IF NOT EXISTS super_stream_partitions INTEGER",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_exchanges', 'super_stream_partitions') IS NULL ALTER TABLE rmq_exchanges ADD super_stream_partitions INT NULL",
            _ => "ALTER TABLE rmq_exchanges ADD COLUMN super_stream_partitions INTEGER"
        };

        return ExecuteIgnoreDuplicateColumnAsync(sql, ct);
    }

    private async Task EnsureRabbitStreamRetentionColumnsAsync(CancellationToken ct)
    {
        var countSql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_queues ADD COLUMN IF NOT EXISTS stream_max_length_messages BIGINT",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_queues', 'stream_max_length_messages') IS NULL ALTER TABLE rmq_queues ADD stream_max_length_messages BIGINT NULL",
            _ => "ALTER TABLE rmq_queues ADD COLUMN stream_max_length_messages INTEGER"
        };

        var lengthSql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_queues ADD COLUMN IF NOT EXISTS stream_max_length_bytes BIGINT",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_queues', 'stream_max_length_bytes') IS NULL ALTER TABLE rmq_queues ADD stream_max_length_bytes BIGINT NULL",
            _ => "ALTER TABLE rmq_queues ADD COLUMN stream_max_length_bytes INTEGER"
        };

        var ageSql = _provider switch
        {
            DatabaseProvider.Postgres => "ALTER TABLE rmq_queues ADD COLUMN IF NOT EXISTS stream_max_age_ms BIGINT",
            DatabaseProvider.MsSql => "IF COL_LENGTH('rmq_queues', 'stream_max_age_ms') IS NULL ALTER TABLE rmq_queues ADD stream_max_age_ms BIGINT NULL",
            _ => "ALTER TABLE rmq_queues ADD COLUMN stream_max_age_ms INTEGER"
        };

        await ExecuteIgnoreDuplicateColumnAsync(countSql, ct);
        await ExecuteIgnoreDuplicateColumnAsync(lengthSql, ct);
        await ExecuteIgnoreDuplicateColumnAsync(ageSql, ct);
    }

    private async Task EnsureRabbitStreamPublisherTableAsync(CancellationToken ct)
    {
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "CREATE TABLE IF NOT EXISTS rmq_stream_publishers (queue_name TEXT NOT NULL, publisher_ref TEXT NOT NULL, last_sequence BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (queue_name, publisher_ref))",
            DatabaseProvider.MsSql => "IF OBJECT_ID('rmq_stream_publishers', 'U') IS NULL CREATE TABLE rmq_stream_publishers (queue_name NVARCHAR(256) NOT NULL, publisher_ref NVARCHAR(256) NOT NULL, last_sequence BIGINT NOT NULL DEFAULT 0, CONSTRAINT PK_rmq_stream_publishers_compat PRIMARY KEY (queue_name, publisher_ref))",
            _ => "CREATE TABLE IF NOT EXISTS rmq_stream_publishers (queue_name TEXT NOT NULL, publisher_ref TEXT NOT NULL, last_sequence INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (queue_name, publisher_ref))"
        };

        await _db.ExecuteAsync(sql, ct: ct);
    }

    private async Task ExecuteIgnoreDuplicateColumnAsync(string sql, CancellationToken ct)
    {
        try
        {
            await _db.ExecuteAsync(sql, ct: ct);
        }
        catch (Exception ex) when (_provider == DatabaseProvider.Sqlite &&
                                   ex.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
        {
        }
    }

    private async Task JetStreamWriteLoopAsync()
    {
        if (_jsWriteChannel == null) return;
        var reader = _jsWriteChannel.Reader;
        var batch = new List<JetStreamWrite>(_jsBatchSize);

        while (await reader.WaitToReadAsync())
        {
            if (!reader.TryRead(out var first)) continue;
            batch.Add(first);

            var delayTask = Task.Delay(_jsBatchMaxDelay);
            while (batch.Count < _jsBatchSize)
            {
                while (reader.TryRead(out var item))
                {
                    batch.Add(item);
                    if (batch.Count >= _jsBatchSize) break;
                }
                if (batch.Count >= _jsBatchSize) break;

                var waitTask = reader.WaitToReadAsync().AsTask();
                var completed = await Task.WhenAny(waitTask, delayTask);
                if (completed == delayTask) break;
                if (!waitTask.Result) break;
            }

            await PersistJetStreamBatchAsync(batch);
            batch.Clear();
        }
    }

    private async Task PersistJetStreamBatchAsync(List<JetStreamWrite> batch)
    {
        if (batch.Count == 0) return;
        try
        {
            await _db.ExecuteAsync("BEGIN IMMEDIATE;");
            foreach (var item in batch)
            {
                var rows = await _db.QueryAsync(
                    "INSERT INTO mq_messages (subject, payload, stream_name) VALUES (@subject, @payload, @stream); SELECT last_insert_rowid() as id;",
                    new[] {
                        SqlParameter.Named("subject", SqlValue.From(item.Subject)),
                        SqlParameter.Named("payload", SqlValue.From(item.Payload)),
                        SqlParameter.Named("stream", SqlValue.From(item.StreamName))
                    });
                var id = rows[0]["id"].AsInt() ?? 0;
                item.Tcs.TrySetResult(id);
            }
            await _db.ExecuteAsync("COMMIT;");
        }
        catch (Exception ex)
        {
            try { await _db.ExecuteAsync("ROLLBACK;"); } catch { }
            foreach (var item in batch)
            {
                item.Tcs.TrySetException(ex);
            }
        }
    }

    private async Task RabbitWriteLoopAsync()
    {
        if (_rmqWriteChannel == null) return;
        var reader = _rmqWriteChannel.Reader;
        var batch = new List<RabbitWrite>(_rmqBatchSize);

        while (await reader.WaitToReadAsync())
        {
            if (!reader.TryRead(out var first)) continue;
            batch.Add(first);

            var delayTask = Task.Delay(_rmqBatchMaxDelay);
            while (batch.Count < _rmqBatchSize)
            {
                while (reader.TryRead(out var item))
                {
                    batch.Add(item);
                    if (batch.Count >= _rmqBatchSize) break;
                }
                if (batch.Count >= _rmqBatchSize) break;

                var waitTask = reader.WaitToReadAsync().AsTask();
                var completed = await Task.WhenAny(waitTask, delayTask);
                if (completed == delayTask) break;
                if (!waitTask.Result) break;
            }

            await PersistRabbitBatchAsync(batch);
            batch.Clear();
        }
    }

    private async Task PersistRabbitBatchAsync(List<RabbitWrite> batch)
    {
        if (batch.Count == 0) return;
        await _sqliteRabbitWriteLock.WaitAsync();
        try
        {
            await _db.ExecuteAsync("BEGIN IMMEDIATE;");
            var completions = new List<(TaskCompletionSource<long> Tcs, long Id)>(batch.Count);
            foreach (var item in batch)
            {
                var rows = await _db.QueryAsync(
                    "INSERT INTO rmq_messages (queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key) VALUES (@queue, @routing, @payload, @headers, @properties, @priority, @expires, @death, @exchange, @originalRouting); SELECT last_insert_rowid() as id;",
                    new[]
                    {
                        SqlParameter.Named("queue", SqlValue.From(item.ScopedQueue)),
                        SqlParameter.Named("routing", SqlValue.From(item.Message.RoutingKey)),
                        SqlParameter.Named("payload", SqlValue.From(item.Message.Payload)),
                        SqlParameter.Named("headers", SqlValue.From(item.Headers ?? string.Empty)),
                        SqlParameter.Named("properties", SqlValue.From(item.Properties ?? string.Empty)),
                        SqlParameter.Named("priority", SqlValue.From((int)item.Message.Priority)),
                        SqlParameter.Named("expires", SqlValue.From(item.ExpiresAt ?? string.Empty)),
                        SqlParameter.Named("death", SqlValue.From(item.Message.DeathCount)),
                        SqlParameter.Named("exchange", SqlValue.From(item.Message.OriginalExchange ?? string.Empty)),
                        SqlParameter.Named("originalRouting", SqlValue.From(item.Message.OriginalRoutingKey ?? string.Empty))
                    });
                completions.Add((item.Tcs, rows[0]["id"].AsInt() ?? 0));
            }
            await _db.ExecuteAsync("COMMIT;");
            foreach (var (tcs, id) in completions)
                tcs.TrySetResult(id);
        }
        catch (Exception ex)
        {
            try { await _db.ExecuteAsync("ROLLBACK;"); } catch { }
            foreach (var item in batch)
                item.Tcs.TrySetException(ex);
        }
        finally
        {
            _sqliteRabbitWriteLock.Release();
        }
    }

    private async Task RabbitDeleteLoopAsync()
    {
        if (_rmqDeleteChannel == null) return;
        var reader = _rmqDeleteChannel.Reader;
        var batch = new List<RabbitDelete>(_rmqBatchSize);

        while (await reader.WaitToReadAsync())
        {
            if (!reader.TryRead(out var first)) continue;
            batch.Add(first);

            var delayTask = Task.Delay(_rmqBatchMaxDelay);
            while (batch.Count < _rmqBatchSize)
            {
                while (reader.TryRead(out var item))
                {
                    batch.Add(item);
                    if (batch.Count >= _rmqBatchSize) break;
                }
                if (batch.Count >= _rmqBatchSize) break;

                var waitTask = reader.WaitToReadAsync().AsTask();
                var completed = await Task.WhenAny(waitTask, delayTask);
                if (completed == delayTask) break;
                if (!waitTask.Result) break;
            }

            await PersistRabbitDeleteBatchAsync(batch);
            batch.Clear();
        }
    }

    private async Task PersistRabbitDeleteBatchAsync(List<RabbitDelete> batch)
    {
        if (batch.Count == 0) return;
        await _sqliteRabbitWriteLock.WaitAsync();
        try
        {
            await _db.ExecuteAsync("BEGIN IMMEDIATE;");
            foreach (var item in batch)
            {
                await _db.ExecuteAsync(
                    "DELETE FROM rmq_messages WHERE id = @id",
                    new[] { SqlParameter.Named("id", SqlValue.From(item.Id)) });
            }
            await _db.ExecuteAsync("COMMIT;");
        }
        catch (Exception ex)
        {
            try { await _db.ExecuteAsync("ROLLBACK;"); } catch { }
            Console.Error.WriteLine($"[Persist] Rabbit delete batch failed: {ex.Message}");
        }
        finally
        {
            _sqliteRabbitWriteLock.Release();
        }
    }

    public async Task FlushAsync(CancellationToken ct = default)
    {
        if (_jsWriteChannel == null || _jsWriteLoop == null) return;
        _jsWriteChannel.Writer.TryComplete();
        _rmqWriteChannel?.Writer.TryComplete();
        _rmqDeleteChannel?.Writer.TryComplete();
        using var reg = ct.Register(() =>
        {
            _jsWriteChannel.Writer.TryComplete();
            _rmqWriteChannel?.Writer.TryComplete();
            _rmqDeleteChannel?.Writer.TryComplete();
        });
        try
        {
            await _jsWriteLoop;
            if (_rmqWriteLoop != null)
                await _rmqWriteLoop;
            if (_rmqDeleteLoop != null)
                await _rmqDeleteLoop;
        }
        catch { }
    }

    private static int ResolveJetStreamBatchSize(int? overrideValue)
    {
        if (overrideValue.HasValue && overrideValue.Value > 0) return overrideValue.Value;
        if (int.TryParse(Environment.GetEnvironmentVariable("COSMOBROKER_JS_BATCH_SIZE"), out var env) && env > 0)
            return env;
        return 128;
    }

    private static TimeSpan ResolveJetStreamBatchDelay(int? overrideMs)
    {
        if (overrideMs.HasValue && overrideMs.Value >= 0) return TimeSpan.FromMilliseconds(overrideMs.Value);
        if (int.TryParse(Environment.GetEnvironmentVariable("COSMOBROKER_JS_BATCH_DELAY_MS"), out var env) && env >= 0)
            return TimeSpan.FromMilliseconds(env);
        return TimeSpan.FromMilliseconds(2);
    }

    private static int ResolveRabbitBatchSize()
    {
        if (int.TryParse(Environment.GetEnvironmentVariable("COSMOBROKER_RMQ_BATCH_SIZE"), out var env) && env > 0)
            return env;
        return 128;
    }

    private static TimeSpan ResolveRabbitBatchDelay()
    {
        if (int.TryParse(Environment.GetEnvironmentVariable("COSMOBROKER_RMQ_BATCH_DELAY_MS"), out var env) && env >= 0)
            return TimeSpan.FromMilliseconds(env);
        return TimeSpan.Zero;
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

    public async Task<PersistedMessage?> GetLastJetStreamMessageBySubjectAsync(string streamName, string subject, CancellationToken ct = default)
    {
        var query = _provider == DatabaseProvider.MsSql
            ? "SELECT TOP 1 id, subject, payload FROM mq_messages WHERE stream_name = @stream AND subject = @subject ORDER BY id DESC"
            : "SELECT id, subject, payload FROM mq_messages WHERE stream_name = @stream AND subject = @subject ORDER BY id DESC LIMIT 1";
        var rows = await _db.QueryAsync(query, new[] {
            SqlParameter.Named("stream", SqlValue.From(streamName)),
            SqlParameter.Named("subject", SqlValue.From(subject))
        }, ct: ct);
        return rows.Select(row => new PersistedMessage {
            Id = row["id"].AsInt() ?? 0,
            Subject = row["subject"].AsString() ?? string.Empty,
            Payload = row["payload"].AsBytes() ?? Array.Empty<byte>()
        }).FirstOrDefault();
    }

    public Task SaveRabbitExchangeAsync(string name, string type, bool durable, bool autoDelete, int? superStreamPartitions = null, CancellationToken ct = default)
        => SaveRabbitExchangeAsync("/", name, type, durable, autoDelete, superStreamPartitions, ct);

    public async Task SaveRabbitExchangeAsync(string vhost, string name, string type, bool durable, bool autoDelete, int? superStreamPartitions = null, CancellationToken ct = default)
    {
        string scopedName = EncodeRabbitScopedName(vhost, name);
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_exchanges (name, type, durable, auto_delete, super_stream_partitions) VALUES (@name, @type, @durable, @autoDelete, @superStreamPartitions) " +
                                         "ON CONFLICT(name) DO UPDATE SET type = EXCLUDED.type, durable = EXCLUDED.durable, auto_delete = EXCLUDED.auto_delete, super_stream_partitions = EXCLUDED.super_stream_partitions",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM rmq_exchanges WHERE name = @name) " +
                                      "UPDATE rmq_exchanges SET type = @type, durable = @durable, auto_delete = @autoDelete, super_stream_partitions = @superStreamPartitions WHERE name = @name " +
                                      "ELSE INSERT INTO rmq_exchanges (name, type, durable, auto_delete, super_stream_partitions) VALUES (@name, @type, @durable, @autoDelete, @superStreamPartitions)",
            _ => "INSERT INTO rmq_exchanges (name, type, durable, auto_delete, super_stream_partitions) VALUES (@name, @type, @durable, @autoDelete, @superStreamPartitions) " +
                 "ON CONFLICT(name) DO UPDATE SET type = excluded.type, durable = excluded.durable, auto_delete = excluded.auto_delete, super_stream_partitions = excluded.super_stream_partitions"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("name", SqlValue.From(scopedName)),
            SqlParameter.Named("type", SqlValue.From(type)),
            SqlParameter.Named("durable", SqlValue.From(durable)),
            SqlParameter.Named("autoDelete", SqlValue.From(autoDelete)),
            SqlParameter.Named("superStreamPartitions", SqlValue.From(superStreamPartitions ?? 0))
        }, ct: ct);
    }

    public Task DeleteRabbitExchangeAsync(string name, CancellationToken ct = default)
        => DeleteRabbitExchangeAsync("/", name, ct);

    public async Task DeleteRabbitExchangeAsync(string vhost, string name, CancellationToken ct = default)
    {
        string scopedName = EncodeRabbitScopedName(vhost, name);
        await _db.ExecuteAsync("DELETE FROM rmq_bindings WHERE exchange_name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
        await _db.ExecuteAsync("DELETE FROM rmq_exchanges WHERE name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
    }

    public Task SaveRabbitQueueAsync(string name, RabbitQueueArgs args, CancellationToken ct = default)
        => SaveRabbitQueueAsync("/", name, args, ct);

    public async Task SaveRabbitQueueAsync(string vhost, string name, RabbitQueueArgs args, CancellationToken ct = default)
    {
        string scopedName = EncodeRabbitScopedName(vhost, name);
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_queues (name, queue_type, durable, exclusive, auto_delete, max_priority, dead_letter_exchange, dead_letter_routing_key, message_ttl_ms, queue_ttl_ms, stream_max_length_messages, stream_max_length_bytes, stream_max_age_ms) " +
                                         "VALUES (@name, @queueType, @durable, @exclusive, @autoDelete, @maxPriority, @dlx, @dlrk, @msgTtl, @queueTtl, @streamMaxLengthMessages, @streamMaxLengthBytes, @streamMaxAgeMs) " +
                                         "ON CONFLICT(name) DO UPDATE SET queue_type = EXCLUDED.queue_type, durable = EXCLUDED.durable, exclusive = EXCLUDED.exclusive, auto_delete = EXCLUDED.auto_delete, max_priority = EXCLUDED.max_priority, dead_letter_exchange = EXCLUDED.dead_letter_exchange, dead_letter_routing_key = EXCLUDED.dead_letter_routing_key, message_ttl_ms = EXCLUDED.message_ttl_ms, queue_ttl_ms = EXCLUDED.queue_ttl_ms, stream_max_length_messages = EXCLUDED.stream_max_length_messages, stream_max_length_bytes = EXCLUDED.stream_max_length_bytes, stream_max_age_ms = EXCLUDED.stream_max_age_ms",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM rmq_queues WHERE name = @name) " +
                                      "UPDATE rmq_queues SET queue_type = @queueType, durable = @durable, exclusive = @exclusive, auto_delete = @autoDelete, max_priority = @maxPriority, dead_letter_exchange = @dlx, dead_letter_routing_key = @dlrk, message_ttl_ms = @msgTtl, queue_ttl_ms = @queueTtl, stream_max_length_messages = @streamMaxLengthMessages, stream_max_length_bytes = @streamMaxLengthBytes, stream_max_age_ms = @streamMaxAgeMs WHERE name = @name " +
                                      "ELSE INSERT INTO rmq_queues (name, queue_type, durable, exclusive, auto_delete, max_priority, dead_letter_exchange, dead_letter_routing_key, message_ttl_ms, queue_ttl_ms, stream_max_length_messages, stream_max_length_bytes, stream_max_age_ms) VALUES (@name, @queueType, @durable, @exclusive, @autoDelete, @maxPriority, @dlx, @dlrk, @msgTtl, @queueTtl, @streamMaxLengthMessages, @streamMaxLengthBytes, @streamMaxAgeMs)",
            _ => "INSERT INTO rmq_queues (name, queue_type, durable, exclusive, auto_delete, max_priority, dead_letter_exchange, dead_letter_routing_key, message_ttl_ms, queue_ttl_ms, stream_max_length_messages, stream_max_length_bytes, stream_max_age_ms) " +
                 "VALUES (@name, @queueType, @durable, @exclusive, @autoDelete, @maxPriority, @dlx, @dlrk, @msgTtl, @queueTtl, @streamMaxLengthMessages, @streamMaxLengthBytes, @streamMaxAgeMs) " +
                 "ON CONFLICT(name) DO UPDATE SET queue_type = excluded.queue_type, durable = excluded.durable, exclusive = excluded.exclusive, auto_delete = excluded.auto_delete, max_priority = excluded.max_priority, dead_letter_exchange = excluded.dead_letter_exchange, dead_letter_routing_key = excluded.dead_letter_routing_key, message_ttl_ms = excluded.message_ttl_ms, queue_ttl_ms = excluded.queue_ttl_ms, stream_max_length_messages = excluded.stream_max_length_messages, stream_max_length_bytes = excluded.stream_max_length_bytes, stream_max_age_ms = excluded.stream_max_age_ms"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("name", SqlValue.From(scopedName)),
            SqlParameter.Named("queueType", SqlValue.From(args.Type.ToString().ToLowerInvariant())),
            SqlParameter.Named("durable", SqlValue.From(args.Durable)),
            SqlParameter.Named("exclusive", SqlValue.From(args.Exclusive)),
            SqlParameter.Named("autoDelete", SqlValue.From(args.AutoDelete)),
            SqlParameter.Named("maxPriority", SqlValue.From((int)args.MaxPriority)),
            SqlParameter.Named("dlx", SqlValue.From(args.DeadLetterExchange ?? string.Empty)),
            SqlParameter.Named("dlrk", SqlValue.From(args.DeadLetterRoutingKey ?? string.Empty)),
            SqlParameter.Named("msgTtl", SqlValue.From(args.MessageTtlMs.HasValue ? args.MessageTtlMs.Value : 0)),
            SqlParameter.Named("queueTtl", SqlValue.From(args.QueueTtlMs.HasValue ? args.QueueTtlMs.Value : 0)),
            SqlParameter.Named("streamMaxLengthMessages", SqlValue.From(args.StreamMaxLengthMessages.HasValue ? args.StreamMaxLengthMessages.Value : 0L)),
            SqlParameter.Named("streamMaxLengthBytes", SqlValue.From(args.StreamMaxLengthBytes.HasValue ? args.StreamMaxLengthBytes.Value : 0L)),
            SqlParameter.Named("streamMaxAgeMs", SqlValue.From(args.StreamMaxAgeMs.HasValue ? args.StreamMaxAgeMs.Value : 0L))
        }, ct: ct);
    }

    public Task DeleteRabbitQueueAsync(string name, CancellationToken ct = default)
        => DeleteRabbitQueueAsync("/", name, ct);

    public async Task DeleteRabbitQueueAsync(string vhost, string name, CancellationToken ct = default)
    {
        string scopedName = EncodeRabbitScopedName(vhost, name);
        await _db.ExecuteAsync("DELETE FROM rmq_messages WHERE queue_name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
        await _db.ExecuteAsync("DELETE FROM rmq_stream_consumers WHERE queue_name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
        await _db.ExecuteAsync("DELETE FROM rmq_bindings WHERE queue_name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
        await _db.ExecuteAsync("DELETE FROM rmq_queues WHERE name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(scopedName)) }, ct: ct);
    }

    public Task SaveRabbitBindingAsync(string exchangeName, string queueName, string routingKey, Dictionary<string, string>? headerArgs, CancellationToken ct = default)
        => SaveRabbitBindingAsync("/", exchangeName, queueName, routingKey, headerArgs, BindingDestinationKind.Queue, ct);

    public async Task SaveRabbitBindingAsync(string vhost, string exchangeName, string queueName, string routingKey, Dictionary<string, string>? headerArgs, BindingDestinationKind destinationKind = BindingDestinationKind.Queue, CancellationToken ct = default)
    {
        string headerJson = headerArgs == null ? string.Empty : JsonSerializer.Serialize(headerArgs);
        string scopedExchange = EncodeRabbitScopedName(vhost, exchangeName);
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_bindings (exchange_name, queue_name, routing_key, destination_kind, header_args) VALUES (@exchange, @queue, @routing, @kind, @headers) " +
                                         "ON CONFLICT(exchange_name, queue_name, routing_key) DO UPDATE SET destination_kind = EXCLUDED.destination_kind, header_args = EXCLUDED.header_args",
            DatabaseProvider.MsSql => "IF EXISTS (SELECT 1 FROM rmq_bindings WHERE exchange_name = @exchange AND queue_name = @queue AND routing_key = @routing) " +
                                      "UPDATE rmq_bindings SET destination_kind = @kind, header_args = @headers WHERE exchange_name = @exchange AND queue_name = @queue AND routing_key = @routing " +
                                      "ELSE INSERT INTO rmq_bindings (exchange_name, queue_name, routing_key, destination_kind, header_args) VALUES (@exchange, @queue, @routing, @kind, @headers)",
            _ => "INSERT INTO rmq_bindings (exchange_name, queue_name, routing_key, destination_kind, header_args) VALUES (@exchange, @queue, @routing, @kind, @headers) " +
                 "ON CONFLICT(exchange_name, queue_name, routing_key) DO UPDATE SET destination_kind = excluded.destination_kind, header_args = excluded.header_args"
        };

        await _db.ExecuteAsync(sql, new[] {
            SqlParameter.Named("exchange", SqlValue.From(scopedExchange)),
            SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
            SqlParameter.Named("routing", SqlValue.From(routingKey)),
            SqlParameter.Named("kind", SqlValue.From(destinationKind.ToString().ToLowerInvariant())),
            SqlParameter.Named("headers", SqlValue.From(headerJson))
        }, ct: ct);
    }

    public Task DeleteRabbitBindingAsync(string exchangeName, string queueName, string routingKey, CancellationToken ct = default)
        => DeleteRabbitBindingAsync("/", exchangeName, queueName, routingKey, BindingDestinationKind.Queue, ct);

    public async Task DeleteRabbitBindingAsync(string vhost, string exchangeName, string queueName, string routingKey, BindingDestinationKind destinationKind = BindingDestinationKind.Queue, CancellationToken ct = default)
    {
        string scopedExchange = EncodeRabbitScopedName(vhost, exchangeName);
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);
        await _db.ExecuteAsync(
            "DELETE FROM rmq_bindings WHERE exchange_name = @exchange AND queue_name = @queue AND routing_key = @routing AND destination_kind = @kind",
            new[] {
                SqlParameter.Named("exchange", SqlValue.From(scopedExchange)),
                SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
                SqlParameter.Named("routing", SqlValue.From(routingKey)),
                SqlParameter.Named("kind", SqlValue.From(destinationKind.ToString().ToLowerInvariant()))
            }, ct: ct);
    }

    public async Task DeleteRabbitBindingsForDestinationAsync(string vhost, string exchangeName, string destinationName, BindingDestinationKind destinationKind = BindingDestinationKind.Queue, CancellationToken ct = default)
    {
        string scopedExchange = EncodeRabbitScopedName(vhost, exchangeName);
        string scopedDestination = EncodeRabbitScopedName(vhost, destinationName);
        await _db.ExecuteAsync(
            "DELETE FROM rmq_bindings WHERE exchange_name = @exchange AND queue_name = @destination AND destination_kind = @kind",
            new[] {
                SqlParameter.Named("exchange", SqlValue.From(scopedExchange)),
                SqlParameter.Named("destination", SqlValue.From(scopedDestination)),
                SqlParameter.Named("kind", SqlValue.From(destinationKind.ToString().ToLowerInvariant()))
            }, ct: ct);
    }

    public Task<long> EnqueueRabbitMessageAsync(string queueName, RabbitMessage message, bool immediate = false, CancellationToken ct = default)
        => EnqueueRabbitMessageAsync("/", queueName, message, immediate, ct);

    public async Task<long> EnqueueRabbitMessageAsync(string vhost, string queueName, RabbitMessage message, bool immediate = false, CancellationToken ct = default)
    {
        string? headers = message.Headers == null ? null : JsonSerializer.Serialize(message.Headers);
        string? properties = JsonSerializer.Serialize(message.Properties);
        string? expiresAt = message.ExpiresAt?.ToString("O");
        string scopedQueue = EncodeRabbitScopedName(vhost, queueName);

        if (_provider == DatabaseProvider.Sqlite && immediate)
        {
            await _sqliteRabbitWriteLock.WaitAsync(ct);
            try
            {
                var immediateRows = await _db.QueryAsync(
                    "INSERT INTO rmq_messages (queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key) VALUES (@queue, @routing, @payload, @headers, @properties, @priority, @expires, @death, @exchange, @originalRouting); SELECT last_insert_rowid() as id;",
                    new[] {
                        SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
                        SqlParameter.Named("routing", SqlValue.From(message.RoutingKey)),
                        SqlParameter.Named("payload", SqlValue.From(message.Payload)),
                        SqlParameter.Named("headers", SqlValue.From(headers ?? string.Empty)),
                        SqlParameter.Named("properties", SqlValue.From(properties ?? string.Empty)),
                        SqlParameter.Named("priority", SqlValue.From((int)message.Priority)),
                        SqlParameter.Named("expires", SqlValue.From(expiresAt ?? string.Empty)),
                        SqlParameter.Named("death", SqlValue.From(message.DeathCount)),
                        SqlParameter.Named("exchange", SqlValue.From(message.OriginalExchange ?? string.Empty)),
                        SqlParameter.Named("originalRouting", SqlValue.From(message.OriginalRoutingKey ?? string.Empty))
                    }, ct: ct);
                return immediateRows[0]["id"].AsInt() ?? 0;
            }
            finally
            {
                _sqliteRabbitWriteLock.Release();
            }
        }

        if (_provider == DatabaseProvider.Sqlite && _rmqWriteChannel != null)
        {
            var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            await _rmqWriteChannel.Writer.WriteAsync(
                new RabbitWrite(scopedQueue, message, headers, properties, expiresAt, tcs),
                ct);
            return await tcs.Task.WaitAsync(ct);
        }

        var sql = _provider switch
        {
            DatabaseProvider.Postgres => "INSERT INTO rmq_messages (queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key) VALUES (@queue, @routing, @payload, @headers, @properties, @priority, @expires, @death, @exchange, @originalRouting) RETURNING id",
            DatabaseProvider.MsSql => "INSERT INTO rmq_messages (queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key) VALUES (@queue, @routing, @payload, @headers, @properties, @priority, @expires, @death, @exchange, @originalRouting); SELECT CAST(SCOPE_IDENTITY() AS BIGINT) as id;",
            _ => "INSERT INTO rmq_messages (queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key) VALUES (@queue, @routing, @payload, @headers, @properties, @priority, @expires, @death, @exchange, @originalRouting); SELECT last_insert_rowid() as id;"
        };

        var rows = await _db.QueryAsync(sql, new[] {
            SqlParameter.Named("queue", SqlValue.From(scopedQueue)),
            SqlParameter.Named("routing", SqlValue.From(message.RoutingKey)),
            SqlParameter.Named("payload", SqlValue.From(message.Payload)),
            SqlParameter.Named("headers", SqlValue.From(headers ?? string.Empty)),
            SqlParameter.Named("properties", SqlValue.From(properties ?? string.Empty)),
            SqlParameter.Named("priority", SqlValue.From((int)message.Priority)),
            SqlParameter.Named("expires", SqlValue.From(expiresAt ?? string.Empty)),
            SqlParameter.Named("death", SqlValue.From(message.DeathCount)),
            SqlParameter.Named("exchange", SqlValue.From(message.OriginalExchange ?? string.Empty)),
            SqlParameter.Named("originalRouting", SqlValue.From(message.OriginalRoutingKey ?? string.Empty))
        }, ct: ct);

        return rows[0]["id"].AsInt() ?? 0;
    }

    public async Task DeleteRabbitMessageAsync(long id, CancellationToken ct = default)
    {
        if (_provider == DatabaseProvider.Sqlite && _rmqDeleteChannel != null)
        {
            if (!_rmqDeleteChannel.Writer.TryWrite(new RabbitDelete(id)))
                await _rmqDeleteChannel.Writer.WriteAsync(new RabbitDelete(id), ct);
            return;
        }

        await _db.ExecuteAsync("DELETE FROM rmq_messages WHERE id = @id",
            new[] { SqlParameter.Named("id", SqlValue.From(id)) }, ct: ct);
    }

    public async Task<List<PersistedRabbitExchange>> GetRabbitExchangesAsync(CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT name, type, durable, auto_delete, super_stream_partitions FROM rmq_exchanges ORDER BY name", ct: ct);
        return rows.Select(row => new PersistedRabbitExchange
        {
            Vhost = DecodeRabbitScopedName(row["name"].AsString()).Vhost,
            Name = DecodeRabbitScopedName(row["name"].AsString()).Name,
            Type = row["type"].AsString() ?? string.Empty,
            Durable = AsBool(row["durable"]),
            AutoDelete = AsBool(row["auto_delete"]),
            SuperStreamPartitions = NullIfZero(row["super_stream_partitions"].AsInt())
        }).ToList();
    }

    public async Task<List<PersistedRabbitQueue>> GetRabbitQueuesAsync(CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT name, queue_type, durable, exclusive, auto_delete, max_priority, dead_letter_exchange, dead_letter_routing_key, message_ttl_ms, queue_ttl_ms, stream_max_length_messages, stream_max_length_bytes, stream_max_age_ms FROM rmq_queues ORDER BY name", ct: ct);
        return rows.Select(row => new PersistedRabbitQueue
        {
            Vhost = DecodeRabbitScopedName(row["name"].AsString()).Vhost,
            Name = DecodeRabbitScopedName(row["name"].AsString()).Name,
            Type = Enum.TryParse<CosmoBroker.RabbitMQ.RabbitQueueType>(row["queue_type"].AsString(), true, out var queueType)
                ? queueType
                : CosmoBroker.RabbitMQ.RabbitQueueType.Classic,
            Durable = AsBool(row["durable"]),
            Exclusive = AsBool(row["exclusive"]),
            AutoDelete = AsBool(row["auto_delete"]),
            MaxPriority = (byte)(row["max_priority"].AsInt() ?? 0),
            DeadLetterExchange = NullIfEmpty(row["dead_letter_exchange"].AsString()),
            DeadLetterRoutingKey = NullIfEmpty(row["dead_letter_routing_key"].AsString()),
            MessageTtlMs = NullIfZero(row["message_ttl_ms"].AsInt()),
            QueueTtlMs = NullIfZero(row["queue_ttl_ms"].AsInt()),
            StreamMaxLengthMessages = NullIfZeroLong(row["stream_max_length_messages"].AsInt()),
            StreamMaxLengthBytes = NullIfZeroLong(row["stream_max_length_bytes"].AsInt()),
            StreamMaxAgeMs = NullIfZeroLong(row["stream_max_age_ms"].AsInt())
        }).ToList();
    }

    public async Task<List<PersistedRabbitBinding>> GetRabbitBindingsAsync(CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT exchange_name, queue_name, routing_key, destination_kind, header_args FROM rmq_bindings ORDER BY exchange_name, queue_name, routing_key", ct: ct);
        return rows.Select(row => new PersistedRabbitBinding
        {
            Vhost = DecodeRabbitScopedName(row["exchange_name"].AsString()).Vhost,
            ExchangeName = DecodeRabbitScopedName(row["exchange_name"].AsString()).Name,
            DestinationName = DecodeRabbitScopedName(row["queue_name"].AsString()).Name,
            RoutingKey = row["routing_key"].AsString() ?? string.Empty,
            DestinationKind = Enum.TryParse<BindingDestinationKind>(row["destination_kind"].AsString(), true, out var destinationKind)
                ? destinationKind
                : BindingDestinationKind.Queue,
            HeaderArgs = DeserializeHeaders(row["header_args"].AsString())
        }).ToList();
    }

    public async Task<List<PersistedRabbitMessage>> GetRabbitMessagesAsync(CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT id, queue_name, routing_key, payload, headers, properties, priority, expires_at, death_count, original_exchange, original_routing_key, created_at FROM rmq_messages ORDER BY queue_name, id", ct: ct);
        return rows.Select(row => new PersistedRabbitMessage
        {
            Id = row["id"].AsInt() ?? 0,
            Vhost = DecodeRabbitScopedName(row["queue_name"].AsString()).Vhost,
            QueueName = DecodeRabbitScopedName(row["queue_name"].AsString()).Name,
            RoutingKey = row["routing_key"].AsString() ?? string.Empty,
            Payload = row["payload"].AsBytes() ?? Array.Empty<byte>(),
            Headers = DeserializeHeaders(row["headers"].AsString()),
            Properties = DeserializeRabbitProperties(row["properties"].AsString()),
            Priority = (byte)(row["priority"].AsInt() ?? 0),
            ExpiresAt = ParseDate(row["expires_at"].AsString()),
            CreatedAt = ParseDate(row["created_at"].AsString()) ?? DateTime.UtcNow,
            DeathCount = (int)(row["death_count"].AsInt() ?? 0),
            OriginalExchange = NullIfEmpty(row["original_exchange"].AsString()),
            OriginalRoutingKey = NullIfEmpty(row["original_routing_key"].AsString())
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

    private static string JoinCsv(IReadOnlyCollection<string>? values)
    {
        if (values == null || values.Count == 0) return string.Empty;
        return string.Join(",", values.Where(v => !string.IsNullOrWhiteSpace(v)).Select(v => v.Trim()));
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

    private static bool AsBool(dynamic value)
    {
        return (value.AsInt() ?? 0) != 0 || string.Equals(value.AsString(), "true", StringComparison.OrdinalIgnoreCase);
    }

    private static int? NullIfZero(long? value) => value is null or 0 ? null : (int)value.Value;
    private static long? NullIfZeroLong(long? value) => value is null or 0 ? null : value.Value;

    private static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private static Dictionary<string, string>? DeserializeHeaders(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return null;
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(json);
        }
        catch
        {
            return null;
        }
    }

    private static RabbitMessageProperties DeserializeRabbitProperties(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return new RabbitMessageProperties();
        try
        {
            return JsonSerializer.Deserialize<RabbitMessageProperties>(json) ?? new RabbitMessageProperties();
        }
        catch
        {
            return new RabbitMessageProperties();
        }
    }

    private static DateTime? ParseDate(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        return DateTime.TryParse(value, out var parsed) ? parsed : null;
    }

    private static string EncodeRabbitScopedName(string? vhost, string name)
    {
        var resolvedVhost = string.IsNullOrWhiteSpace(vhost) ? "/" : vhost!.Trim();
        return $"{resolvedVhost}\u001F{name}";
    }

    private static (string Vhost, string Name) DecodeRabbitScopedName(string? scopedName)
    {
        if (string.IsNullOrEmpty(scopedName))
            return ("/", string.Empty);

        int separator = scopedName.IndexOf('\u001F');
        if (separator < 0)
            return ("/", scopedName);

        return (scopedName[..separator], scopedName[(separator + 1)..]);
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

public class RabbitPermissions
{
    public required string Username { get; set; }
    public string DefaultVhost { get; set; } = "/";
    public List<string> AllowedVhosts { get; set; } = new();
    public List<string> ConfigurePatterns { get; set; } = new();
    public List<string> WritePatterns { get; set; } = new();
    public List<string> ReadPatterns { get; set; } = new();
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

public class PersistedRabbitExchange
{
    public string Vhost { get; set; } = "/";
    public required string Name { get; set; }
    public required string Type { get; set; }
    public bool Durable { get; set; }
    public bool AutoDelete { get; set; }
    public int? SuperStreamPartitions { get; set; }
}

public class PersistedRabbitQueue
{
    public string Vhost { get; set; } = "/";
    public required string Name { get; set; }
    public CosmoBroker.RabbitMQ.RabbitQueueType Type { get; set; }
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public byte MaxPriority { get; set; }
    public string? DeadLetterExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public int? MessageTtlMs { get; set; }
    public int? QueueTtlMs { get; set; }
    public long? StreamMaxLengthMessages { get; set; }
    public long? StreamMaxLengthBytes { get; set; }
    public long? StreamMaxAgeMs { get; set; }
}

public class PersistedRabbitBinding
{
    public string Vhost { get; set; } = "/";
    public required string ExchangeName { get; set; }
    public required string DestinationName { get; set; }
    public required string RoutingKey { get; set; }
    public BindingDestinationKind DestinationKind { get; set; }
    public Dictionary<string, string>? HeaderArgs { get; set; }
}

public enum BindingDestinationKind
{
    Queue,
    Exchange
}

public class PersistedRabbitMessage
{
    public long Id { get; set; }
    public string Vhost { get; set; } = "/";
    public required string QueueName { get; set; }
    public required string RoutingKey { get; set; }
    public required byte[] Payload { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public RabbitMessageProperties Properties { get; set; } = new();
    public byte Priority { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ExpiresAt { get; set; }
    public int DeathCount { get; set; }
    public string? OriginalExchange { get; set; }
    public string? OriginalRoutingKey { get; set; }
}

internal sealed record JetStreamWrite(string StreamName, string Subject, byte[] Payload, TaskCompletionSource<long> Tcs);
internal sealed record RabbitWrite(string ScopedQueue, RabbitMessage Message, string? Headers, string? Properties, string? ExpiresAt, TaskCompletionSource<long> Tcs);
internal sealed record RabbitDelete(long Id);
