using System.Net;
using System.Text;
using System.Web;
using System.Linq;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.Zip;

internal static class Program
{
    private const string Prefix = "http://localhost:5050/";
    private static readonly string FilesRoot =
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "files"));

    private static readonly ConcurrentDictionary<string, CacheEntry> Cache = new(StringComparer.OrdinalIgnoreCase);

    private static readonly int Cpu = Math.Max(1, Environment.ProcessorCount);
    private static readonly SemaphoreSlim ZipGate = new(initialCount: Cpu, maxCount: Cpu);

    private static readonly ReaderWriterLockSlim LogLock = new();
    private static readonly string LogDir = Path.Combine(AppContext.BaseDirectory, "logs");
    private static string LogFile => Path.Combine(LogDir, $"server-{DateTime.UtcNow:yyyy-MM-dd}.log");

    private sealed record CacheEntry(byte[] Bytes, string ContentType, DateTimeOffset CreatedUtc, string ETag);

    private static async Task Main()
    {
        Directory.CreateDirectory(LogDir);
        Directory.CreateDirectory(FilesRoot);

        using var listener = new HttpListener();
        listener.Prefixes.Add(Prefix);
        listener.Start();
        Log($"START {Prefix} root={FilesRoot}");

        _ = Task.Run(async () =>
        {
            while (listener.IsListening)
            {
                HttpListenerContext ctx;
                try { ctx = await listener.GetContextAsync().ConfigureAwait(false); }
                catch (HttpListenerException) { break; }
                catch (ObjectDisposedException) { break; }

                _ = Task.Run(() => Handle(ctx));
            }
        });

        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    private static async Task Handle(HttpListenerContext ctx)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        res.ContentEncoding = Encoding.UTF8;

        var rawPath = req.Url?.AbsolutePath ?? "/";
        if (rawPath == "/") { await WriteUsage(res); return; }

        var selector = rawPath.Trim('/');

        if (selector.StartsWith("_purge", StringComparison.OrdinalIgnoreCase))
        {
            await HandlePurge(selector, res);
            return;
        }

        var files = selector.Split('&', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                            .Select(HttpUtility.UrlDecode)
                            .Where(s => !string.IsNullOrWhiteSpace(s))
                            .Select(NormalizeSafeRelativePath)
                            .Where(s => s is not null)
                            .Select(s => s!)
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                            .ToArray();

        if (files.Length == 0)
        {
            await WriteText(res, 400, "Bad request: no file names provided.");
            return;
        }

        var key = string.Join('&', files);
        Log($"REQ {req.RemoteEndPoint} -> [{key}]");

        if (Cache.TryGetValue(key, out var cached))
        {
            var inm = req.Headers["If-None-Match"];
            if (!string.IsNullOrEmpty(inm) && string.Equals(inm, cached.ETag, StringComparison.Ordinal))
            {
                res.StatusCode = 304;
                res.AddHeader("ETag", cached.ETag);
                res.Close();
                Log($"HIT304 [{key}]");
                return;
            }

            await WriteBytes(res, 200, cached.Bytes, cached.ContentType, cached.ETag);
            Log($"HIT [{key}] {cached.Bytes.Length}B");
            return;
        }

        var fulls = new List<(string rel, string full)>();
        foreach (var rel in files)
        {
            var full = Path.GetFullPath(Path.Combine(FilesRoot, rel));
            if (IsUnderRoot(full) && File.Exists(full))
                fulls.Add((rel, full));
        }

        if (fulls.Count == 0)
        {
            await WriteText(res, 404, "Nijedan traženi fajl ne postoji na serveru.");
            Log($"MISS-NONE [{key}]");
            return;
        }

        byte[] zipBytes;
        var gateTaken = false;
        try
        {
            await ZipGate.WaitAsync().ConfigureAwait(false);
            gateTaken = true;
            zipBytes = await Task.Run(() => BuildZipInMemory(fulls)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await WriteText(res, 500, "Internal error during ZIP creation.");
            Log($"ERR ZIP [{key}] {ex}");
            return;
        }
        finally
        {
            if (gateTaken) ZipGate.Release();
        }

        var etag = ComputeETag(zipBytes, key);
        var entry = new CacheEntry(zipBytes, "application/zip", DateTimeOffset.UtcNow, etag);
        Cache[key] = entry;

        await WriteBytes(res, 200, zipBytes, entry.ContentType, entry.ETag);
        Log($"MISS->CACHED [{key}] {zipBytes.Length}B");
    }

    private static async Task HandlePurge(string selector, HttpListenerResponse res)
    {
        var parts = selector.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 1)
        {
            var n = Cache.Count;
            Cache.Clear();
            await WriteText(res, 200, $"Keš obrisan. Stavki: {n}.");
            Log($"PURGE-ALL {n}");
            return;
        }

        var rest = string.Join('/', parts.Skip(1));
        var keys = rest.Split('&', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                       .Select(HttpUtility.UrlDecode)
                       .Where(s => !string.IsNullOrWhiteSpace(s))
                       .Select(NormalizeSafeRelativePath)
                       .Where(s => s is not null)
                       .Select(s => s!)
                       .Distinct(StringComparer.OrdinalIgnoreCase)
                       .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                       .ToArray();

        if (keys.Length == 0)
        {
            await WriteText(res, 400, "Bad purge selector.");
            return;
        }

        var key = string.Join('&', keys);
        var removed = Cache.TryRemove(key, out _);
        await WriteText(res, 200, removed ? $"Obrisan: [{key}]." : $"Nije nađen: [{key}].");
        Log($"PURGE-ONE [{key}] removed={removed}");
    }

    private static async Task WriteUsage(HttpListenerResponse res)
    {
        var sb = new StringBuilder();
        sb.AppendLine("ZipServer");
        sb.AppendLine($"Root folder: {FilesRoot}");
        sb.AppendLine();
        sb.AppendLine("Upotreba:");
        sb.AppendLine("  GET /file1.txt&file2.pdf            -> vraca ZIP sa postojecim fajlovima");
        sb.AppendLine("  GET /_purge                         -> brise ceo keš");
        sb.AppendLine("  GET /_purge/file1.txt&file2.pdf     -> brise konkretan keš ključ");
        sb.AppendLine();
        sb.AppendLine("Napomena: ako bar jedan fajl postoji, ZIP se pravi sa tim fajlovima; nepostojeći se preskaču.");
        await WriteText(res, 200, sb.ToString());
    }

    private static async Task WriteText(HttpListenerResponse res, int status, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        res.StatusCode = status;
        res.ContentType = "text/plain; charset=utf-8";
        res.ContentLength64 = bytes.Length;
        await res.OutputStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        res.Close();
    }

    private static async Task WriteBytes(HttpListenerResponse res, int status, byte[] bytes, string contentType, string etag)
    {
        res.StatusCode = status;
        res.ContentType = contentType;
        res.ContentLength64 = bytes.LongLength;
        res.AddHeader("ETag", etag);
        res.AddHeader("Content-Disposition", $"attachment; filename=\"bundle.zip\"");
        await res.OutputStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        res.Close();
    }

    private static string? NormalizeSafeRelativePath(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return null;
        var trimmed = input.Replace('\\', '/').Trim();
        if (trimmed.StartsWith("/")) trimmed = trimmed.TrimStart('/');
        if (trimmed.Contains("..", StringComparison.Ordinal)) return null;
        return trimmed;
    }

    private static bool IsUnderRoot(string fullPath)
    {
        var root = Path.GetFullPath(FilesRoot).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var full = Path.GetFullPath(fullPath);
        return full.StartsWith(root, StringComparison.OrdinalIgnoreCase);
    }

    private static byte[] BuildZipInMemory(List<(string rel, string full)> files)
    {
        using var ms = new MemoryStream();
        using (var zip = new ZipOutputStream(ms))
        {
            zip.IsStreamOwner = false; 
            zip.SetLevel(6);         

            foreach (var (rel, full) in files)
            {
                var fi = new FileInfo(full);
                var entry = new ZipEntry(rel)
                {
                    DateTime = fi.LastWriteTime,
                    Size = fi.Length
                };
                zip.PutNextEntry(entry);

                using var fs = new FileStream(full, FileMode.Open, FileAccess.Read, FileShare.Read);
                fs.CopyTo(zip); 
                zip.CloseEntry();
            }
            zip.Finish();
        }
        return ms.ToArray();
    }

    private static string ComputeETag(byte[] data, string key)
    {
        using var sha = SHA256.Create();
        var h1 = sha.ComputeHash(data);
        var h2 = sha.ComputeHash(Encoding.UTF8.GetBytes(key));
        for (int i = 0; i < h1.Length; i++) h1[i] ^= h2[i % h2.Length];
        return "\"" + Convert.ToHexString(h1) + "\"";
    }

    private static void Log(string line)
    {
        var msg = $"{DateTime.UtcNow:O} {line}";
        try
        {
            LogLock.EnterWriteLock();
            File.AppendAllText(LogFile, msg + Environment.NewLine, Encoding.UTF8);
        }
        finally
        {
            if (LogLock.IsWriteLockHeld) LogLock.ExitWriteLock();
        }
        Console.WriteLine(msg);
    }
}
