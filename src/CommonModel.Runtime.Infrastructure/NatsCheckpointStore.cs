using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using System.Text.Json;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Configuration;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Infrastructure;

public sealed class NatsCheckpointStore : ICheckpointStore
{
    private readonly NatsOptions _options;
    private readonly NatsConnectionFactory _factory;
    private readonly ILogger<NatsCheckpointStore> _logger;
    private INatsKVStore? _kv;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public NatsCheckpointStore(
        IOptions<NatsOptions> options,
        NatsConnectionFactory factory,
        ILogger<NatsCheckpointStore> logger)
    {
        _options = options.Value;
        _factory = factory;
        _logger  = logger;
    }

    public async Task<Checkpoint?> GetAsync(string driverId, string entityPath, CancellationToken ct = default)
    {
        // Best-effort: connection establishment (GetOrCreateKvAsync) is inside the
        // try so a NATS outage degrades to "no checkpoint" instead of throwing —
        // callers (e.g. AvevaPiAfAdapter.EnsureCookieAsync) treat null as "anchor
        // at current state and keep going" rather than crashing on startup.
        try
        {
            var kv    = await GetOrCreateKvAsync(ct);
            var key   = BuildKey(driverId, entityPath);
            var entry = await kv.GetEntryAsync<byte[]>(key, cancellationToken: ct);
            if (entry.Value is null) return null;
            return JsonSerializer.Deserialize<Checkpoint>(entry.Value);
        }
        catch (NatsKVKeyNotFoundException)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read checkpoint for {DriverId}/{EntityPath}", driverId, entityPath);
            return null;
        }
    }

    public async Task SaveAsync(Checkpoint checkpoint, CancellationToken ct = default)
    {
        // Best-effort: see GetAsync. A NATS outage logs a warning and is swallowed
        // — the in-memory position is authoritative; persistence just makes it
        // survive restarts.
        try
        {
            var kv    = await GetOrCreateKvAsync(ct);
            var key   = BuildKey(checkpoint.DriverId, checkpoint.EntityPath);
            var bytes = JsonSerializer.SerializeToUtf8Bytes(checkpoint);
            await kv.PutAsync(key, bytes, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save checkpoint for {DriverId}/{EntityPath}",
                checkpoint.DriverId, checkpoint.EntityPath);
        }
    }

    private async ValueTask<INatsKVStore> GetOrCreateKvAsync(CancellationToken ct)
    {
        if (_kv is not null) return _kv;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_kv is not null) return _kv;

            var conn  = await _factory.GetSharedConnectionAsync(ct);
            var js    = new NatsJSContext(conn);
            var kvCtx = new NatsKVContext(js);
            _kv = await GetOrCreateBucketAsync(kvCtx, _options.CheckpointBucket, ct);

            _logger.LogInformation("Checkpoint KV bucket '{Bucket}' ready", _options.CheckpointBucket);
        }
        finally
        {
            _initLock.Release();
        }

        return _kv!;
    }

    private static async Task<INatsKVStore> GetOrCreateBucketAsync(
        NatsKVContext kvCtx, string bucket, CancellationToken ct)
    {
        try
        {
            return await kvCtx.CreateStoreAsync(new NatsKVConfig(bucket), ct);
        }
        catch
        {
            return await kvCtx.GetStoreAsync(bucket, ct);
        }
    }

    // NATS KV keys accept [A-Za-z0-9._=/-]. PI AF entity paths can contain
    // spaces and backslashes ("element/\\Aveva-Pi\BKO_LULU_DB\BKO Test1"),
    // so we normalize: '\' → '/', then strip anything outside the allowed set.
    private static string BuildKey(string driverId, string entityPath)
    {
        var raw = $"{driverId}.{entityPath}".ToLowerInvariant();
        var sb  = new System.Text.StringBuilder(raw.Length);
        foreach (var c in raw)
        {
            if (c == '\\') sb.Append('/');
            else if (c == ':' || c == ' ') sb.Append('-');
            else if (char.IsLetterOrDigit(c) || c == '.' || c == '_' || c == '-' || c == '/' || c == '=')
                sb.Append(c);
            else
                sb.Append('_');
        }
        return sb.ToString();
    }
}
