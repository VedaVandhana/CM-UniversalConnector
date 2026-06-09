// AVEVA PI System Explorer adapter — connects to PI Asset Framework via the
// official OSIsoft.AFSDK and supports bidirectional sync of element templates
// and elements. Server identity, AF system name, AF database, and the
// authentication mode are read entirely from the connector descriptor — no
// server-specific code anywhere in this class.
//
// To use this adapter the host machine (or container) must have PI AF Client
// installed and OSIsoft.AFSDK.dll on the assembly probe path. Activating a
// second PI AF server is purely a config exercise: drop another descriptor
// yaml file in `connectors/` pointing at the new server.

using Microsoft.Extensions.Logging;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.PI;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using CommonModel.Runtime.Core.Abstractions;
using CommonModel.Runtime.Core.Descriptors;
using CommonModel.Runtime.Core.Models;

namespace CommonModel.Runtime.Drivers.Generic.Adapters;

public sealed class AvevaPiAfAdapter : BaseProtocolAdapter, IWritableProtocolAdapter
{
    public const string SourceTypeName    = "avevapi-af";
    public const string EntityTypeTemplate          = "elementTemplate";
    public const string EntityTypeElement           = "element";
    public const string EntityTypeAttribute         = "attribute";
    public const string EntityTypeAttributeTemplate = "attributeTemplate";
    public const string EntityTypePiPoint           = "piPoint";
    public const string ReplicaSessionHeader = "replicaSession";
    private const string CookieEntityPath = "afcookie";

    // PI Data Archive (PIServer) connections — separate from AF. Resolved
    // lazily on first piPoint operation; one PIServer per descriptor.
    private readonly Dictionary<string, PIServer> _piServers = new(StringComparer.OrdinalIgnoreCase);

    private readonly ILogger<AvevaPiAfAdapter> _logger;
    private readonly ICheckpointStore _checkpoints;

    // One PISystem connection is shared across all descriptors that target the
    // same AF server. The adapter is a singleton, so we cache by AF system name.
    private readonly Dictionary<string, PISystem> _systems = new(StringComparer.OrdinalIgnoreCase);

    // AF Server-side change cookies — per driver. AFDatabase.FindChangedItems
    // returns a cookie that resumes the next call from where this one left off.
    // On first open per driver, we either restore the cookie from the checkpoint
    // store (survives restarts) or anchor it to "now" via GetFindChangedItemsCookie
    // — never start from null, which would replay the entire AF change-buffer
    // history on every cold start.
    private readonly Dictionary<string, object?> _changeCookies = new();

    // Tracks replica-session IDs we have applied via the reverse path so that the
    // forward poll can drop the resulting echo CDC event (loop prevention L1).
    private readonly HashSet<string> _selfWriteSessions = new(StringComparer.OrdinalIgnoreCase);

    public AvevaPiAfAdapter(ILogger<AvevaPiAfAdapter> logger, ICheckpointStore checkpoints)
    {
        _logger      = logger;
        _checkpoints = checkpoints;
    }

    public override string SourceType => SourceTypeName;

    public IReadOnlyList<string> SupportedEntityTypes =>
        new[]
        {
            EntityTypeTemplate,
            EntityTypeElement,
            EntityTypeAttribute,
            EntityTypeAttributeTemplate,
            EntityTypePiPoint
        };

    // ─── Open / close ────────────────────────────────────────────────────────

    protected override async Task OpenCoreAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var systemName = ResolveSystemName(descriptor);
        if (!_systems.TryGetValue(systemName, out var system))
        {
            var systems = new PISystems();
            system = systems[systemName]
                ?? throw new InvalidOperationException(
                    $"PI AF system '{systemName}' is not registered on this host. " +
                    $"Add it in PI System Explorer or via `afdiag /pisystem:{systemName}`.");

            var c = descriptor.Connection;
            if (!string.IsNullOrWhiteSpace(c.Username))
                system.Connect(new System.Net.NetworkCredential(c.Username, c.Password ?? ""));
            else
                system.Connect();

            _systems[systemName] = system;
            _logger.LogInformation(
                "Connected to PI AF system '{System}' (server: {Server}, version: {Version})",
                systemName, system.Name, system.ServerVersion);
        }

        await EnsureCookieAsync(descriptor, ct);
    }

    // Per-driver cookie initialization — runs once per driver lifetime.
    //
    // CRITICAL: The cookie used with AFDatabase.FindChangedItems is a *database*
    // cookie, NOT the system cookie returned by PISystem.GetFindChangedItemsCookie.
    // Passing a system cookie to AFDatabase.FindChangedItems will silently produce
    // wrong results (no further changes detected after the first poll).
    //
    // Anchor order:
    //   1. Already in-memory → no-op.
    //   2. Restore from checkpoint store (KV bucket cm-checkpoints) → resume
    //      exactly where we left off across restarts.
    //   3. Anchor by calling FindChangedItems(false, int.MaxValue, null, out cookie)
    //      and DISCARDING the returned items — this is the AVEVA-documented way
    //      to get a database cookie that means "everything from now forward".
    //      Drain in a loop to make sure we are truly at the buffer tail.
    private async Task EnsureCookieAsync(ConnectorDescriptor descriptor, CancellationToken ct)
    {
        var driverId = descriptor.DriverId;
        if (_changeCookies.ContainsKey(driverId)) return;

        var checkpoint = await _checkpoints.GetAsync(driverId, CookieEntityPath, ct);
        if (checkpoint is not null && TryDeserializeCookie(checkpoint.Position, out var restored))
        {
            _changeCookies[driverId] = restored;
            _logger.LogInformation(
                "Restored PI AF change-cookie for driver '{Driver}' from checkpoint (updated {When:o})",
                driverId, checkpoint.UpdatedAt);
            return;
        }

        // No persisted cookie — drain the database's change buffer to anchor at
        // its current tail. The returned changes are intentionally discarded so
        // pre-existing commits don't replay through the pipeline.
        var db = ResolveDatabase(descriptor);
        object? anchor = null;
        int discarded = 0;
        while (true)
        {
            var results = db.FindChangedItems(false, 1000, anchor, out var next);
            anchor = next;
            if (results is null || results.Count == 0) break;
            discarded += results.Count;
        }

        _changeCookies[driverId] = anchor;
        _logger.LogInformation(
            "Anchored PI AF change-cookie for driver '{Driver}' to current database tail " +
            "(discarded {Discarded} pre-existing change record(s) from the AF buffer).",
            driverId, discarded);

        await TrySaveCookieAsync(driverId, anchor, ct);
    }

    protected override Task CloseCoreAsync(CancellationToken ct)
    {
        foreach (var sys in _systems.Values)
        {
            try { sys.Disconnect(); } catch { /* swallow */ }
        }
        _systems.Clear();
        foreach (var srv in _piServers.Values)
        {
            try { srv.Disconnect(); } catch { /* swallow */ }
        }
        _piServers.Clear();
        return Task.CompletedTask;
    }

    // ─── Forward path: poll for template / element changes ──────────────────

    public override async IAsyncEnumerable<RawChangeRecord> StreamRawChangesAsync(
        ConnectorDescriptor descriptor,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(descriptor.ChangeDetection.PollIntervalSeconds, 5));
        var db = ResolveDatabase(descriptor);
        var watchTypes = ResolveWatchedTypes(descriptor);

        // One-time full-tree snapshot on connect: the change-cookie API only
        // returns *deltas*, so the broker would otherwise never learn the
        // elements that existed before it started. Emitting every element once
        // (ChangeType.Snapshot) lets the broker mirror the *entire* live PI AF
        // tree, exactly as PSE shows it.
        foreach (var rec in EmitFullTree(db, watchTypes, ct))
        {
            if (ct.IsCancellationRequested) yield break;
            yield return rec;
        }

        while (!ct.IsCancellationRequested)
        {
            foreach (var rec in DrainChanges(db, descriptor.DriverId, watchTypes))
            {
                if (rec.AdapterMetadata.TryGetValue(ReplicaSessionHeader, out var sid) &&
                    _selfWriteSessions.Contains(sid))
                {
                    _selfWriteSessions.Remove(sid);
                    continue;
                }
                yield return rec;
            }

            await TrySaveCookieAsync(descriptor.DriverId, _changeCookies[descriptor.DriverId], ct);

            try { await Task.Delay(interval, ct); }
            catch (OperationCanceledException) { yield break; }
        }
    }

    // AFDatabase.FindChangedItems is the official server-side delta API: it
    // returns only items changed since the previous cookie. We drain in pages
    // until the server reports no more changes for this round.
    //
    // searchSandbox: false → committed server-side changes only. Setting this
    // to true would surface the calling SDK session's own pending check-outs.
    private IEnumerable<RawChangeRecord> DrainChanges(
        AFDatabase db, string driverId, HashSet<string> watchTypes)
    {
        var key = driverId;
        _changeCookies.TryGetValue(key, out var cookie);

        const int pageSize = 500;
        int totalFromAf = 0;
        int totalEmitted = 0;
        var emittedPaths = new List<string>();
        var skippedReasons = new List<string>();

        while (true)
        {
            var changes = db.FindChangedItems(false, pageSize, cookie, out var nextCookie);
            cookie = nextCookie;
            if (changes is null || changes.Count == 0) break;
            totalFromAf += changes.Count;

            foreach (var ci in changes)
            {
                var rec = ToRecord(db, ci, watchTypes);
                if (rec is null)
                {
                    skippedReasons.Add($"{ci.Identity}/{ci.Action}/{ci.ID}");
                    continue;
                }
                _logger.LogDebug(
                    "PI AF yielding {ChangeType} for {EntityPath} (driver={Driver})",
                    rec.ChangeType, rec.EntityPath, driverId);
                yield return rec;
                totalEmitted++;
                emittedPaths.Add(rec.EntityPath);
            }

            if (changes.Count < pageSize) break;
        }

        _changeCookies[key] = cookie;

        if (totalFromAf > 0)
            _logger.LogDebug(
                "PI AF poll for '{Driver}': AF returned {AfCount} change(s), emitted {Emitted} " +
                "[{Paths}]; skipped {Skipped} [{SkipReasons}].",
                driverId, totalFromAf, totalEmitted,
                string.Join(", ", emittedPaths),
                totalFromAf - totalEmitted,
                string.Join(", ", skippedReasons));
        else
            _logger.LogDebug("PI AF poll for '{Driver}': no changes.", driverId);
    }

    // Walk the whole AF element tree (depth-first) and emit one Snapshot record
    // per element so the broker learns the entire current structure on connect.
    // Children are loaded lazily by the AF SDK as we descend; this is a one-time
    // pass per stream lifecycle. el.GetPath() gives the full ``\\PISystem\DB\..``
    // path, which encodes the area/system nesting the broker rebuilds from.
    private IEnumerable<RawChangeRecord> EmitFullTree(
        AFDatabase db, HashSet<string> watchTypes, CancellationToken ct)
    {
        if (!watchTypes.Contains(EntityTypeElement))
            yield break;

        var ts = DateTimeOffset.UtcNow;
        var stack = new Stack<AFElement>();
        foreach (AFElement top in db.Elements)
            stack.Push(top);

        int emitted = 0;
        while (stack.Count > 0 && !ct.IsCancellationRequested)
        {
            var el = stack.Pop();
            foreach (AFElement child in el.Elements)
                stack.Push(child);

            yield return new RawChangeRecord
            {
                EntityPath      = $"{EntityTypeElement}/{el.GetPath()}",
                ChangeType      = ChangeType.Snapshot,
                SourceTimestamp = ts,
                Fields          = BuildElementFields(el),
                AdapterMetadata = new Dictionary<string, string>
                {
                    ["source"]     = SourceTypeName,
                    ["entityType"] = EntityTypeElement,
                    // Marks this as a full-tree snapshot, not a change. The
                    // GenericConnector rewrites ChangeType.Snapshot→Insert/Update
                    // before publishing, so the broker keys on this flag to cache
                    // the element (for the hierarchy tree) without fanning a
                    // canonical create/update across the mesh.
                    ["snapshot"]   = "true",
                },
            };
            emitted++;
        }
        _logger.LogInformation("PI AF full-tree snapshot emitted {Count} element(s).", emitted);
    }

    // ─── Cookie persistence ─────────────────────────────────────────────────
    //
    // The AF SDK returns the cookie as an opaque `object` (concrete type varies
    // by AF SDK version — typically string or a small DTO). We serialize via
    // XmlSerializer (the same approach the legacy 4.8 connector used), prefix
    // with the AssemblyQualifiedName so the loader can recover the type, and
    // store the result as a base64 string in Checkpoint.Position.
    //
    // All operations are best-effort: a serialization failure logs a warning
    // and falls back to in-memory cookie only. The connector keeps polling.

    private async Task TrySaveCookieAsync(string driverId, object? cookie, CancellationToken ct)
    {
        if (cookie is null) return;
        var serialized = SerializeCookie(cookie);
        if (serialized is null) return;

        try
        {
            await _checkpoints.SaveAsync(new Checkpoint
            {
                DriverId   = driverId,
                EntityPath = CookieEntityPath,
                Position   = serialized
            }, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to persist PI AF change-cookie for driver '{Driver}' — " +
                "next restart will re-anchor to current server state.", driverId);
        }
    }

    private string? SerializeCookie(object cookie)
    {
        try
        {
            var type = cookie.GetType();
            var ser  = new XmlSerializer(type);
            using var sw = new StringWriter();
            ser.Serialize(sw, cookie);
            var bytes = Encoding.UTF8.GetBytes(sw.ToString());
            return $"{type.AssemblyQualifiedName}|{Convert.ToBase64String(bytes)}";
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "PI AF change-cookie is not XML-serializable ({Type}) — checkpoint persistence disabled.",
                cookie.GetType().FullName);
            return null;
        }
    }

    private bool TryDeserializeCookie(string? position, out object? cookie)
    {
        cookie = null;
        if (string.IsNullOrWhiteSpace(position)) return false;
        var sep = position.IndexOf('|');
        if (sep <= 0) return false;

        try
        {
            var type = Type.GetType(position[..sep]);
            if (type is null) return false;
            var xml = Encoding.UTF8.GetString(Convert.FromBase64String(position[(sep + 1)..]));
            var ser = new XmlSerializer(type);
            using var sr = new StringReader(xml);
            cookie = ser.Deserialize(sr);
            return cookie is not null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to deserialize persisted PI AF change-cookie — will re-anchor to current state.");
            return false;
        }
    }

    private static RawChangeRecord? ToRecord(AFDatabase db, AFChangeInfo info, HashSet<string> watchTypes)
    {
        var changeType = info.Action switch
        {
            AFChangeInfoAction.Added   => ChangeType.Insert,
            AFChangeInfoAction.Updated => ChangeType.Update,
            AFChangeInfoAction.Removed => ChangeType.Delete,
            _                          => (ChangeType?)null
        };
        if (changeType is null) return null;

        var ts = new DateTimeOffset(info.ChangeTime.UtcTime, TimeSpan.Zero);

        switch (info.Identity)
        {
            case AFIdentity.Element when watchTypes.Contains(EntityTypeElement):
            {
                if (changeType == ChangeType.Delete)
                    return DeletedRecord(EntityTypeElement, info, ts);
                // info.FindObject(piSystem, autoLoad: true) forces a server-side fetch
                // so newly-added objects (not yet in the local AFDatabase collection
                // cache) resolve correctly. db.Elements[info.ID] would miss them.
                var el = info.FindObject(db.PISystem, autoLoad: true) as AFElement;
                if (el is null) return null;
                return new RawChangeRecord
                {
                    EntityPath      = $"{EntityTypeElement}/{el.GetPath()}",
                    ChangeType      = changeType.Value,
                    SourceTimestamp = ts,
                    Fields          = BuildElementFields(el),
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["source"]     = SourceTypeName,
                        ["entityType"] = EntityTypeElement
                    }
                };
            }
            case AFIdentity.ElementTemplate when watchTypes.Contains(EntityTypeTemplate):
            {
                if (changeType == ChangeType.Delete)
                    return DeletedRecord(EntityTypeTemplate, info, ts);
                var t = info.FindObject(db.PISystem, autoLoad: true) as AFElementTemplate;
                if (t is null) return null;
                return new RawChangeRecord
                {
                    EntityPath      = $"{EntityTypeTemplate}/{t.Name}",
                    ChangeType      = changeType.Value,
                    SourceTimestamp = ts,
                    Fields          = BuildTemplateFields(t),
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["source"]     = SourceTypeName,
                        ["entityType"] = EntityTypeTemplate
                    }
                };
            }
            case AFIdentity.Attribute when watchTypes.Contains(EntityTypeAttribute):
            {
                if (changeType == ChangeType.Delete)
                    return DeletedRecord(EntityTypeAttribute, info, ts);
                var attr = info.FindObject(db.PISystem, autoLoad: true) as AFAttribute;
                if (attr is null) return null;
                var ownerPath = attr.Element?.GetPath() ?? attr.Template?.Name ?? "";
                return new RawChangeRecord
                {
                    EntityPath      = $"{EntityTypeAttribute}/{ownerPath}/{attr.Name}",
                    ChangeType      = changeType.Value,
                    SourceTimestamp = ts,
                    Fields          = BuildAttributeFields(attr),
                    AdapterMetadata = new Dictionary<string, string>
                    {
                        ["source"]     = SourceTypeName,
                        ["entityType"] = EntityTypeAttribute
                    }
                };
            }
        }
        return null;
    }

    private static RawChangeRecord DeletedRecord(string entityType, AFChangeInfo info, DateTimeOffset ts) =>
        new()
        {
            EntityPath      = $"{entityType}/{info.ID}",
            ChangeType      = ChangeType.Delete,
            SourceTimestamp = ts,
            Fields          = new Dictionary<string, object?>
            {
                ["uniqueId"] = info.ID.ToString()
            },
            AdapterMetadata = new Dictionary<string, string>
            {
                ["source"]     = SourceTypeName,
                ["entityType"] = entityType
            }
        };

    private static Dictionary<string, object?> BuildTemplateFields(AFElementTemplate t)
    {
        var attrs = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var a in t.AttributeTemplates.Cast<AFAttributeTemplate>())
        {
            attrs[a.Name] = new Dictionary<string, object?>
            {
                ["type"]         = a.Type?.Name,
                ["defaultValue"] = SafeDefaultValue(a),
                ["uom"]          = a.DefaultUOM?.Abbreviation,
                ["isConfig"]     = a.IsConfigurationItem
            };
        }

        return new Dictionary<string, object?>
        {
            ["name"]         = t.Name,
            ["description"]  = t.Description,
            ["baseTemplate"] = t.BaseTemplate?.Name,
            ["categories"]   = t.Categories.Select(c => c.Name).ToList(),
            ["attributes"]   = attrs,
            ["uniqueId"]     = t.UniqueID.ToString()
        };
    }

    private static string? SafeDefaultValue(AFAttributeTemplate a)
    {
        try { return a.GetValue(null)?.ToString(); }
        catch { return null; }
    }

    private static Dictionary<string, object?> BuildAttributeFields(AFAttribute a)
    {
        string? value;
        try   { value = a.GetValue()?.Value?.ToString(); }
        catch { value = null; }

        return new Dictionary<string, object?>
        {
            ["name"]                 = a.Name,
            ["description"]          = a.Description,
            ["type"]                 = a.Type?.Name,
            ["uom"]                  = a.DefaultUOM?.Abbreviation,
            ["value"]                = value,
            ["dataReferencePlugIn"]  = a.DataReferencePlugIn?.Name,
            ["configString"]         = a.ConfigString,
            ["element"]              = a.Element?.GetPath(),
            ["template"]              = a.Template?.Name,
            ["uniqueId"]             = a.UniqueID.ToString()
        };
    }

    private static Dictionary<string, object?> BuildElementFields(AFElement e)
    {
        var values = new Dictionary<string, object?>();
        foreach (var a in e.Attributes)
        {
            try { values[a.Name] = a.GetValue()?.Value?.ToString(); }
            catch { values[a.Name] = null; }
        }

        return new Dictionary<string, object?>
        {
            ["name"]        = e.Name,
            ["description"] = e.Description,
            ["template"]    = e.Template?.Name,
            ["parent"]      = e.Parent?.GetPath(),
            ["path"]        = e.GetPath(),
            ["attributes"]  = values,
            ["categories"]  = e.Categories.Select(c => c.Name).ToList(),
            ["uniqueId"]    = e.UniqueID.ToString()
        };
    }

    private static HashSet<string> ResolveWatchedTypes(ConnectorDescriptor d)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (d.Watch.Entities.Count == 0)
        {
            set.Add(EntityTypeElement);
            set.Add(EntityTypeTemplate);
            set.Add(EntityTypeAttribute);
            return set;
        }
        foreach (var e in d.Watch.Entities)
            set.Add(string.IsNullOrWhiteSpace(e.Filter) ? EntityTypeElement : e.Filter!);
        return set;
    }

    // ─── Reverse path: template + element CRUD ──────────────────────────────

    public async Task<WriteResult> ApplyAsync(
        ConnectorDescriptor descriptor, WriteCommand command, CancellationToken ct)
    {
        await Task.Yield();

        // PI Data Archive (piPoint) commits directly via PIPoint.SaveAttributes —
        // it doesn't participate in the AF database CheckIn / UndoCheckOut
        // transaction. Branch the lifecycle so a PI Point failure can't leak
        // into the AF transaction buffer (and vice versa).
        if (string.Equals(command.EntityType, EntityTypePiPoint, StringComparison.OrdinalIgnoreCase))
            return ApplyPiPointSafe(descriptor, command);

        AFDatabase? db = null;
        try
        {
            db = ResolveDatabase(descriptor);

            // PI AF write lifecycle:
            //   1. Mutate in-memory (Add / Remove / property sets)
            //      AF SDK implicitly checks out each touched object.
            //   2. db.CheckIn() — pushes the pending change set to the AF server.
            //      Until CheckIn runs, NOTHING is persisted; the change exists
            //      only in this client's transaction buffer.
            //   3. On failure, db.UndoCheckOut(true) reverts every dirty object
            //      so the next command starts from a clean transaction.
            var result = command.EntityType switch
            {
                EntityTypeTemplate          => ApplyTemplate         (db, command),
                EntityTypeElement           => ApplyElement          (db, command),
                EntityTypeAttribute         => ApplyAttribute        (db, command),
                EntityTypeAttributeTemplate => ApplyAttributeTemplate(db, command),
                _                           => WriteResult.Fail(
                    $"Unsupported entityType '{command.EntityType}'. " +
                    $"Supported: {string.Join(", ", SupportedEntityTypes)}")
            };

            if (!result.Success)
            {
                TryUndoCheckOut(db, command.CorrelationId);
                return result;
            }

            try
            {
                db.CheckIn();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "PI AF CheckIn failed for {Op} {EntityType} (corr={Corr}) — reverting",
                    command.Operation, command.EntityType, command.CorrelationId);
                TryUndoCheckOut(db, command.CorrelationId);
                return WriteResult.Fail($"CheckIn failed: {ex.Message}");
            }

            var sid = Ulid.NewUlid().ToString();
            _selfWriteSessions.Add(sid);
            _logger.LogInformation(
                "PI AF CheckIn ► {Op} {EntityType} (corr={Corr}, replicaSession={Sid})",
                command.Operation, command.EntityType, command.CorrelationId, sid);
            return WriteResult.Ok(sid, result.Fields);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PI AF write failed for {EntityType} {Pk}",
                command.EntityType, string.Join(",", command.PrimaryKey.Values));
            if (db is not null) TryUndoCheckOut(db, command.CorrelationId);
            return WriteResult.Fail(ex.Message);
        }
    }

    private WriteResult ApplyPiPointSafe(ConnectorDescriptor descriptor, WriteCommand command)
    {
        try
        {
            var result = ApplyPiPoint(descriptor, command);
            if (!result.Success) return result;
            var sid = Ulid.NewUlid().ToString();
            _selfWriteSessions.Add(sid);
            _logger.LogInformation(
                "PI Data Archive ► {Op} {EntityType} {Name} (corr={Corr}, replicaSession={Sid})",
                command.Operation, command.EntityType,
                command.PrimaryKey.TryGetValue("name", out var n) ? n : "?",
                command.CorrelationId, sid);
            return WriteResult.Ok(sid, result.Fields);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PI Data Archive write failed for {EntityType} {Pk}",
                command.EntityType, string.Join(",", command.PrimaryKey.Values));
            return WriteResult.Fail(ex.Message);
        }
    }

    private void TryUndoCheckOut(AFDatabase db, string correlationId)
    {
        try
        {
            db.UndoCheckOut(true);
            _logger.LogWarning(
                "Reverted pending AF check-outs after failure (corr={Corr})",
                correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "UndoCheckOut failed (corr={Corr}) — database may be left with dirty state",
                correlationId);
        }
    }

    private static WriteResult ApplyTemplate(AFDatabase db, WriteCommand cmd)
    {
        var name = ResolveName(cmd, "name");
        var existing = db.ElementTemplates[name];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Template '{name}' already exists.");
                var created = db.ElementTemplates.Add(name);
                ApplyTemplateFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?> { ["uniqueId"] = created.UniqueID.ToString() });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Template '{name}' not found.");
                ApplyTemplateFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?> { ["uniqueId"] = existing.UniqueID.ToString() });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");   // idempotent
                db.ElementTemplates.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static void ApplyTemplateFields(AFElementTemplate template, IReadOnlyDictionary<string, object?> fields)
    {
        if (fields.TryGetValue("description", out var desc))
            template.Description = desc?.ToString() ?? "";

        // Broker bootstrap PATCHes legacy templates to AllowElementToExtend=true
        // so Sensor_<tag> attributes can be added to element instances at restore.
        if (fields.TryGetValue("allowElementToExtend", out var allow) && allow is bool allowBool)
            template.AllowElementToExtend = allowBool;

        if (fields.TryGetValue("baseTemplate", out var bt) && bt is string baseName && baseName.Length > 0)
            template.BaseTemplate = template.Database.ElementTemplates[baseName];

        if (fields.TryGetValue("attributes", out var attrsObj) &&
            attrsObj is IReadOnlyDictionary<string, object?> attrs)
        {
            foreach (var (attrName, spec) in attrs)
            {
                var attr = template.AttributeTemplates[attrName] ?? template.AttributeTemplates.Add(attrName);
                if (spec is IReadOnlyDictionary<string, object?> specMap)
                {
                    if (specMap.TryGetValue("type", out var t) && t is string typeName)
                        attr.Type = Type.GetType(typeName) ?? attr.Type;
                    if (specMap.TryGetValue("defaultValue", out var dv))
                        attr.SetValue(dv, null);
                    if (specMap.TryGetValue("uom", out var uom) && uom is string uomAbbrev)
                        attr.DefaultUOM = template.Database.PISystem.UOMDatabase.UOMs[uomAbbrev];
                    if (specMap.TryGetValue("isConfig", out var ic) && ic is bool isConfig)
                        attr.IsConfigurationItem = isConfig;
                }
            }
        }
    }

    /// <summary>
    /// Resolve a parent element path, creating any missing intermediate
    /// elements (area / system containers). Used when a new element's parent
    /// path doesn't yet exist in AVEVA — e.g. the user dragged a renamed system
    /// template, so "Root\Area\New System" must be materialised before the
    /// equipment can nest under it. Containers are created untemplated; the
    /// whole chain is persisted by the caller's db.CheckIn().
    /// </summary>
    private static AFElement? EnsureParentPath(AFDatabase db, string parentPath)
    {
        var parts = parentPath.Split(new[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return null;
        // Absolute paths (\\PISystem\Database\Root\…) carry a server + database
        // qualifier ahead of the db-relative segments — skip those two.
        var start = parentPath.StartsWith(@"\\") && parts.Length >= 2 ? 2 : 0;
        AFElement? current = null;
        for (var i = start; i < parts.Length; i++)
        {
            var seg = parts[i];
            var next = current is null ? db.Elements[seg] : current.Elements[seg];
            next ??= current is null ? db.Elements.Add(seg) : current.Elements.Add(seg);
            current = next;
        }
        return current;
    }

    private static WriteResult ApplyElement(AFDatabase db, WriteCommand cmd)
    {
        // Path can be supplied either via primary key "path" or fields["path"] / fields["name"]
        var path     = cmd.PrimaryKey.TryGetValue("path", out var p) ? p?.ToString() : null;
        var name     = ResolveName(cmd, "name");
        var existing = !string.IsNullOrWhiteSpace(path)
            ? AFObject.FindObject(path, db) as AFElement
            : db.Elements[name];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Element '{name}' already exists.");
                var templateName = cmd.Fields.TryGetValue("template", out var tpl) ? tpl?.ToString() : null;
                var template = !string.IsNullOrWhiteSpace(templateName)
                    ? db.ElementTemplates[templateName]
                    : null;
                var parentPath = cmd.Fields.TryGetValue("parent", out var pp) ? pp?.ToString() : null;
                AFElement? parent = null;
                if (!string.IsNullOrWhiteSpace(parentPath))
                {
                    // Prefer an existing element at the path; otherwise build the
                    // missing area/system chain so a brand-new system (e.g. a
                    // renamed dragged template) nests under it instead of the
                    // element landing flat at the database root.
                    parent = AFObject.FindObject(parentPath, db) as AFElement
                             ?? EnsureParentPath(db, parentPath);
                }
                var created = parent is null
                    ? db.Elements.Add(name, template)
                    : parent.Elements.Add(name, template);
                ApplyElementFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = created.UniqueID.ToString(),
                    ["path"]     = created.GetPath()
                });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Element '{name}' not found at '{path}'.");
                ApplyElementFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = existing.UniqueID.ToString(),
                    ["path"]     = existing.GetPath()
                });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");
                if (existing.Parent is AFElement parentEl)
                    parentEl.Elements.Remove(existing);
                else
                    db.Elements.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static void ApplyElementFields(AFElement element, IReadOnlyDictionary<string, object?> fields)
    {
        // Identity rename: when `newName` is supplied and differs, change the
        // AF Element Name in-place. PSE's General-tab Name and every path-derived
        // WebId change after CheckIn, so downstream callers must re-resolve.
        // `name` is accepted as an alternate new-name carrier for publishers that
        // put the old name in primary_key.name and the new name in fields.name.
        if (fields.TryGetValue("newName", out var nn) && nn is string newName &&
            newName.Length > 0 && !string.Equals(newName, element.Name, StringComparison.Ordinal))
        {
            element.Name = newName;
        }
        else if (fields.TryGetValue("name", out var nv) && nv is string nameVal &&
                 nameVal.Length > 0 && !string.Equals(nameVal, element.Name, StringComparison.Ordinal))
        {
            element.Name = nameVal;
        }

        if (fields.TryGetValue("description", out var desc))
            element.Description = desc?.ToString() ?? "";

        if (fields.TryGetValue("attributes", out var attrsObj) &&
            attrsObj is IReadOnlyDictionary<string, object?> attrs)
        {
            foreach (var (attrName, value) in attrs)
            {
                var attr = element.Attributes[attrName];
                if (attr is null) continue;
                try { attr.SetValue(new AFValue(value)); }
                catch { /* attribute may be PI-bound — skip silently */ }
            }
        }
    }

    // ─── Reverse path: AF attribute CRUD (per-element) ──────────────────────
    //
    // Element-level attribute CRUD is what the broker's restore_aveva path
    // uses for ``Sensor_<tag>`` attributes (PI-Point-referencing AF attributes).
    // Primary key forms:
    //   { "elementPath": "\\sys\\db\\equip", "name": "Sensor_FE-1011" }
    //   { "elementName": "equip-1", "name": "TagDesc" }
    //
    // Create payload mirrors PI Web API's ``POST /elements/{wid}/attributes``:
    //   { description, type, defaultValue, uom, dataReferencePlugIn, configString, value }
    //
    // When `dataReferencePlugIn == "PI Point"`, `configString` carries the
    // ``\\<DataServer>\<pointName>`` reference and the attribute resolves
    // values transparently through PI Data Archive.
    private static WriteResult ApplyAttribute(AFDatabase db, WriteCommand cmd)
    {
        var attrName = ResolveName(cmd, "name");
        var element  = ResolveElementForAttribute(db, cmd);
        if (element is null)
            return WriteResult.Fail(
                $"Attribute target element not found (primaryKey: {string.Join(",", cmd.PrimaryKey.Keys)}).");

        var existing = element.Attributes[attrName];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Attribute '{attrName}' already exists on '{element.GetPath()}'.");
                var created = element.Attributes.Add(attrName);
                ApplyAttributeFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = created.UniqueID.ToString(),
                    ["element"]  = element.GetPath()
                });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Attribute '{attrName}' not found on '{element.GetPath()}'.");
                ApplyAttributeFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = existing.UniqueID.ToString(),
                    ["element"]  = element.GetPath()
                });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");   // idempotent
                element.Attributes.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static AFElement? ResolveElementForAttribute(AFDatabase db, WriteCommand cmd)
    {
        if (cmd.PrimaryKey.TryGetValue("elementPath", out var ep) && ep is string epath && epath.Length > 0)
            return AFObject.FindObject(epath, db) as AFElement;
        if (cmd.PrimaryKey.TryGetValue("elementName", out var en) && en is string ename && ename.Length > 0)
            return db.Elements[ename];
        if (cmd.Fields.TryGetValue("element", out var fe) && fe is string fpath && fpath.Length > 0)
            return AFObject.FindObject(fpath, db) as AFElement;
        return null;
    }

    private static void ApplyAttributeFields(AFAttribute attr, IReadOnlyDictionary<string, object?> fields)
    {
        if (fields.TryGetValue("description", out var desc))
            attr.Description = desc?.ToString() ?? "";

        if (fields.TryGetValue("type", out var t) && t is string typeName)
            attr.Type = Type.GetType(typeName) ?? attr.Type;

        if (fields.TryGetValue("uom", out var uom) && uom is string uomAbbrev && uomAbbrev.Length > 0)
            attr.DefaultUOM = attr.Database?.PISystem.UOMDatabase.UOMs[uomAbbrev];

        // Order matters: set DataReferencePlugIn first, then ConfigString — the
        // PlugIn parses ConfigString and rejects values it doesn't recognise.
        if (fields.TryGetValue("dataReferencePlugIn", out var drp) && drp is string drpName && drpName.Length > 0)
            attr.DataReferencePlugIn = attr.PISystem.DataReferencePlugIns[drpName];

        if (fields.TryGetValue("configString", out var cs) && cs is string cstr)
            attr.ConfigString = cstr;

        if (fields.TryGetValue("defaultValue", out var dv) && attr.DataReferencePlugIn is null)
            try { attr.SetValue(new AFValue(dv)); } catch { /* incompatible literal */ }

        if (fields.TryGetValue("value", out var v) && attr.DataReferencePlugIn is null)
            try { attr.SetValue(new AFValue(v)); } catch { /* incompatible literal */ }
    }

    // ─── Reverse path: attribute template CRUD (per-template) ───────────────
    //
    // Per-attribute-template ops on an existing element template. PK:
    //   { "templateName": "BkoPump", "name": "AreaId" }
    //
    // The broker's bootstrap uses ``DELETE /attributetemplates/{wid}`` to
    // drop legacy attribute templates (e.g. ``AreaId``) — this is the
    // adapter-side equivalent.
    private static WriteResult ApplyAttributeTemplate(AFDatabase db, WriteCommand cmd)
    {
        if (!cmd.PrimaryKey.TryGetValue("templateName", out var tplObj) ||
            tplObj is not string templateName || templateName.Length == 0)
            return WriteResult.Fail("attributeTemplate primaryKey requires 'templateName'.");

        var template = db.ElementTemplates[templateName];
        if (template is null)
            return WriteResult.Fail($"Template '{templateName}' not found.");

        var attrName = ResolveName(cmd, "name");
        var existing = template.AttributeTemplates[attrName];

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"Attribute template '{attrName}' already exists on '{templateName}'.");
                var created = template.AttributeTemplates.Add(attrName);
                ApplyAttributeTemplateFields(created, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = created.UniqueID.ToString(),
                    ["template"] = templateName
                });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"Attribute template '{attrName}' not found on '{templateName}'.");
                ApplyAttributeTemplateFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["uniqueId"] = existing.UniqueID.ToString(),
                    ["template"] = templateName
                });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");
                template.AttributeTemplates.Remove(existing);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static void ApplyAttributeTemplateFields(AFAttributeTemplate attr, IReadOnlyDictionary<string, object?> fields)
    {
        if (fields.TryGetValue("description", out var desc))
            attr.Description = desc?.ToString() ?? "";
        if (fields.TryGetValue("type", out var t) && t is string typeName)
            attr.Type = Type.GetType(typeName) ?? attr.Type;
        if (fields.TryGetValue("defaultValue", out var dv))
            try { attr.SetValue(dv, null); } catch { /* incompatible literal */ }
        if (fields.TryGetValue("uom", out var uom) && uom is string uomAbbrev && uomAbbrev.Length > 0)
            attr.DefaultUOM = attr.Database?.PISystem.UOMDatabase.UOMs[uomAbbrev];
        if (fields.TryGetValue("isConfig", out var ic) && ic is bool isConfig)
            attr.IsConfigurationItem = isConfig;
    }

    // ─── Reverse path: PI Data Archive (PI Point) CRUD ──────────────────────
    //
    // PI Points live in PI Data Archive, not AF. The AF SDK exposes them via
    // OSIsoft.AF.PI.PIServer / PIPoint. The data server name comes from
    // ``connection.piServerName`` in the descriptor; when blank, we fall back
    // to the AF system name (typical PI single-host installs).
    //
    // PI Point ops commit immediately — no db.CheckIn() — so the lifecycle
    // is branched at ApplyAsync.
    private WriteResult ApplyPiPoint(ConnectorDescriptor descriptor, WriteCommand cmd)
    {
        var server = ResolvePiServer(descriptor);
        var name = ResolveName(cmd, "name");
        var existing = TryFindPiPoint(server, name);

        switch (cmd.Operation)
        {
            case WriteOperation.Create:
                if (existing is not null)
                    return WriteResult.Fail($"PI Point '{name}' already exists on '{server.Name}'.");
                var attrs = BuildPiPointCreateAttributes(cmd.Fields);
                // PIServer.CreatePIPoint(name, IDictionary<string, object>) creates the
                // PI Point and applies the initial attribute set in one round-trip.
                var created = server.CreatePIPoint(name, attrs);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["id"]     = created.ID,
                    ["name"]   = created.Name,
                    ["server"] = server.Name
                });

            case WriteOperation.Update:
                if (existing is null)
                    return WriteResult.Fail($"PI Point '{name}' not found on '{server.Name}'.");
                ApplyPiPointFields(existing, cmd.Fields);
                return WriteResult.Ok("", new Dictionary<string, object?>
                {
                    ["id"]     = existing.ID,
                    ["name"]   = existing.Name,
                    ["server"] = server.Name
                });

            case WriteOperation.Delete:
                if (existing is null) return WriteResult.Ok("");
                server.DeletePIPoint(name);
                return WriteResult.Ok("");

            default:
                return WriteResult.Fail($"Unknown operation {cmd.Operation}");
        }
    }

    private static PIPoint? TryFindPiPoint(PIServer server, string name)
    {
        try   { return PIPoint.FindPIPoint(server, name); }
        catch { return null; }
    }

    // Maps the broker's PI Web API field names to PI Point attribute names.
    // PI Point's native attribute names are case-sensitive and lower-cased
    // (descriptor, pointtype, pointclass, engunits, …) — easy to get wrong.
    private static IDictionary<string, object> BuildPiPointCreateAttributes(
        IReadOnlyDictionary<string, object?> fields)
    {
        var attrs = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        void Add(string piName, object? value)
        {
            if (value is null) return;
            var s = value.ToString();
            if (string.IsNullOrEmpty(s)) return;
            attrs[piName] = s;
        }

        fields.TryGetValue("descriptor", out var desc);          Add("descriptor", desc);
        fields.TryGetValue("engineeringUnits", out var units);   Add("engunits",   units);
        fields.TryGetValue("pointType", out var ptype);          Add("pointtype",  ptype ?? "Float32");
        fields.TryGetValue("pointClass", out var pclass);        Add("pointclass", pclass ?? "classic");

        // extraAttributes is a free-form map for callers that need to set
        // additional native PI Point attributes (e.g. zero/span, compressing,
        // step) at creation time.
        if (fields.TryGetValue("extraAttributes", out var extra) &&
            extra is IReadOnlyDictionary<string, object?> extras)
        {
            foreach (var (k, v) in extras) Add(k, v);
        }
        return attrs;
    }

    private static void ApplyPiPointFields(PIPoint point, IReadOnlyDictionary<string, object?> fields)
    {
        var changed = new List<string>();

        void Set(string piName, object? value)
        {
            if (value is null) return;
            point.SetAttribute(piName, value.ToString() ?? "");
            changed.Add(piName);
        }

        if (fields.TryGetValue("descriptor", out var desc))         Set("descriptor", desc);
        if (fields.TryGetValue("engineeringUnits", out var units))  Set("engunits",   units);
        if (fields.TryGetValue("pointType", out var ptype) && ptype is string pt && pt.Length > 0)
            Set("pointtype", pt);

        if (fields.TryGetValue("extraAttributes", out var extra) &&
            extra is IReadOnlyDictionary<string, object?> extras)
        {
            foreach (var (k, v) in extras) Set(k, v);
        }

        if (changed.Count == 0) return;
        // PIPoint.SaveAttributes(params string[]) flushes the in-memory
        // SetAttribute calls to PI Data Archive in one round-trip.
        point.SaveAttributes(changed.ToArray());
    }

    private PIServer ResolvePiServer(ConnectorDescriptor descriptor)
    {
        // PI Data Archive server name comes from descriptor.connection.piServerName;
        // fall back to AfSystemName for the common single-host PI install.
        var name = descriptor.Connection.PiServerName
                   ?? descriptor.Connection.AfSystemName
                   ?? descriptor.Connection.Host
                   ?? throw new InvalidOperationException(
                       "connection.piServerName (or afSystemName/host) is required for piPoint ops");

        if (_piServers.TryGetValue(name, out var cached) && cached.ConnectionInfo.IsConnected)
            return cached;

        var servers = new PIServers();
        var server = servers[name]
            ?? throw new InvalidOperationException(
                $"PI Data Archive server '{name}' is not registered on this host. " +
                $"Add it via PI System Explorer (Connections → Servers) or PI SDK Utility.");

        var c = descriptor.Connection;
        if (!string.IsNullOrWhiteSpace(c.Username))
            server.Connect(new System.Net.NetworkCredential(c.Username, c.Password ?? ""));
        else
            server.Connect();

        _piServers[name] = server;
        _logger.LogInformation(
            "Connected to PI Data Archive '{Server}' (version: {Version})",
            server.Name, server.ServerVersion);
        return server;
    }

    // ─── Validation ─────────────────────────────────────────────────────────

    public override IReadOnlyList<string> Validate(ConnectorDescriptor descriptor)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(descriptor.Connection.AfSystemName))
            errors.Add("connection.afSystemName is required for avevapi-af");
        if (string.IsNullOrWhiteSpace(descriptor.Connection.AfDatabase))
            errors.Add("connection.afDatabase is required for avevapi-af");
        return errors;
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private PISystem ResolveSystem(ConnectorDescriptor descriptor)
    {
        var name = ResolveSystemName(descriptor);
        if (_systems.TryGetValue(name, out var sys) && sys.ConnectionInfo.IsConnected)
            return sys;
        // Lazy-open if needed (e.g. write command arrived before stream started)
        OpenCoreAsync(descriptor, CancellationToken.None).GetAwaiter().GetResult();
        return _systems[name];
    }

    private AFDatabase ResolveDatabase(ConnectorDescriptor descriptor)
    {
        var system = ResolveSystem(descriptor);
        var dbName = descriptor.Connection.AfDatabase
            ?? throw new InvalidOperationException("connection.afDatabase is required for avevapi-af");
        return system.Databases[dbName]
            ?? throw new InvalidOperationException(
                $"AF database '{dbName}' not found on system '{system.Name}'");
    }

    private static string ResolveSystemName(ConnectorDescriptor descriptor) =>
        descriptor.Connection.AfSystemName
            ?? descriptor.Connection.Host
            ?? throw new InvalidOperationException("connection.afSystemName (or connection.host) is required");

    // Each entity entry is { name: "...", filter?: "template|element" } — filter
    // chooses which AF object class. Defaults to element.
    private static string ResolveName(WriteCommand cmd, string fallbackKey)
    {
        if (cmd.PrimaryKey.TryGetValue("name", out var pk) && pk is string pkName && pkName.Length > 0)
            return pkName;
        if (cmd.Fields.TryGetValue(fallbackKey, out var fb) && fb is string fbName && fbName.Length > 0)
            return fbName;
        throw new InvalidOperationException("Write command must include primaryKey.name or fields.name");
    }

    private static TimeSpan ParseDuration(string iso)
    {
        try { return XmlConvert.ToTimeSpan(iso); }
        catch { return TimeSpan.FromHours(1); }
    }

    public override async ValueTask DisposeAsync()
    {
        foreach (var sys in _systems.Values)
        {
            try { sys.Disconnect(); } catch { }
        }
        _systems.Clear();
        foreach (var srv in _piServers.Values)
        {
            try { srv.Disconnect(); } catch { }
        }
        _piServers.Clear();
        await base.DisposeAsync();
    }
}
