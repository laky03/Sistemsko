using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using ICSharpCode.SharpZipLib.Zip;

internal static class Program
{
    private const string Prefix = "http://localhost:5050/";
    private static readonly string FilesRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "data"));
    private static readonly string LogsDir = Path.Combine(AppContext.BaseDirectory, "logs");

    private static readonly Dictionary<string, CacheEntry> Cache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ReaderWriterLockSlim CacheLock = new(); 

    private static readonly Dictionary<string, object> KeyLocks = new(StringComparer.OrdinalIgnoreCase);
    private static readonly object KeyLocksGuard = new();

    private static StreamWriter? _logWriter;
    private static readonly object LogLock = new();

    private static volatile bool _running = true;

    private sealed class CacheEntry
    {
        public byte[] Data = Array.Empty<byte>();
        public string FileName = "";
        public string ContentType = "application/zip";
        public DateTime CreatedUtc;
        public string Summary = "";
    }

    private static void Main()
    {
        Directory.CreateDirectory(FilesRoot);
        Directory.CreateDirectory(LogsDir);
        _logWriter = new StreamWriter(Path.Combine(LogsDir, $"server-{DateTime.UtcNow:yyyy-MM-dd}.log"), append: true, Encoding.UTF8) { AutoFlush = true };

        var listener = new HttpListener();
        listener.Prefixes.Add(Prefix);

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _running = false;
            try { listener.Stop(); } catch { }
        };

        try
        {
            listener.Start();
            Log($"START {Prefix} | FilesRoot={FilesRoot}");
            Console.WriteLine($"Listening on {Prefix} | FilesRoot={FilesRoot}");
        }
        catch (HttpListenerException ex)
        {
            Log($"FATAL cannot start listener: {ex}");
            return;
        }

        while (_running)
        {
            HttpListenerContext? ctx = null;
            try
            {
                ctx = listener.GetContext(); 
            }
            catch when (!_running) { break; }
            catch (Exception ex)
            {
                Log($"ACCEPT ERROR: {ex}");
                continue;
            }

            ThreadPool.QueueUserWorkItem(_ => ProcessRequest(ctx));
        }

        Log("STOP");
        lock (LogLock) { _logWriter?.Dispose(); _logWriter = null; }
        listener.Close();
    }

    private static void ProcessRequest(HttpListenerContext ctx)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        var remote = SafeRemote(req);

        var rawPath = req.Url?.AbsolutePath ?? "/";
        var decoded = Uri.UnescapeDataString(rawPath).Trim('/');

        if (string.Equals(decoded, "_purge", StringComparison.OrdinalIgnoreCase))
        {
            ClearCacheAll();
            WriteStatus(res, 204, "CACHE_CLEARED");
            LogRequest(remote, rawPath, 204, "CACHE_CLEARED_ALL");
            try { res.OutputStream.Close(); } catch { }
            return;
        }
        if (decoded.StartsWith("_purge/", StringComparison.OrdinalIgnoreCase))
        {
            var target = decoded.Substring("_purge/".Length).Trim();
            var n = string.IsNullOrEmpty(target) ? 0 : ClearCacheByFile(target);
            WriteStatus(res, 204, "CACHE_CLEARED_FILE");
            LogRequest(remote, rawPath, 204, $"CACHE_CLEARED_FILE {target} count={n}");
            try { res.OutputStream.Close(); } catch { }
            return;
        }

        var files = new List<string>();
        if (!string.IsNullOrWhiteSpace(decoded))
        {
            foreach (var part in decoded.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var clean = part.Trim();
                if (clean.Length > 0) files.Add(clean);
            }
        }

        var key = CanonicalKey(files);
        var t0 = DateTime.UtcNow;

        try
        {
            if (files.Count == 0)
            {
                WriteStatus(res, 400, "NO_PARAMS");
                LogRequest(remote, rawPath, 400, "NO_PARAMS");
                return;
            }

            if (TryGetFromCache(key, out var cachedHit))
            {
                WriteZip(res, cachedHit!);
                LogRequest(remote, rawPath, 200, $"CACHE_HIT {cachedHit!.Summary}");
                return;
            }

            object keyLock;
            lock (KeyLocksGuard)
            {
                if (!KeyLocks.TryGetValue(key, out keyLock!))
                {
                    keyLock = new object();
                    KeyLocks[key] = keyLock;
                }
            }

            lock (keyLock)
            {
                if (TryGetFromCache(key, out cachedHit))
                {
                    WriteZip(res, cachedHit!);
                    LogRequest(remote, rawPath, 200, $"CACHE_HIT2 {cachedHit!.Summary}");
                    return;
                }

                var existing = new List<(string Name, string FullPath, long Size)>();
                foreach (var name in files)
                {
                    var full = Path.GetFullPath(Path.Combine(FilesRoot, name));
                    if (!full.StartsWith(FilesRoot, StringComparison.OrdinalIgnoreCase)) continue; 
                    if (File.Exists(full))
                    {
                        var fi = new FileInfo(full);
                        existing.Add((name, full, fi.Length));
                    }
                }

                if (existing.Count == 0)
                {
                    WriteStatus(res, 404, "NO_EXISTING_FILES");
                    LogRequest(remote, rawPath, 404, "NO_EXISTING_FILES");
                    return;
                }

                var (zipName, data, summary) = CreateZip(existing);

                var entry = new CacheEntry
                {
                    Data = data,
                    FileName = zipName,
                    ContentType = "application/zip",
                    CreatedUtc = DateTime.UtcNow,
                    Summary = summary
                };

                PutToCache(key, entry);

                WriteZip(res, entry);
                LogRequest(remote, rawPath, 200, $"CACHE_MISS {entry.Summary}");
            }
        }
        catch (Exception ex)
        {
            try { WriteStatus(res, 500, "INTERNAL_ERROR"); } catch { }
            LogRequest(remote, rawPath, 500, $"ERROR {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            try { res.OutputStream.Close(); } catch { }
            var dt = (DateTime.UtcNow - t0).TotalMilliseconds;
            Log($"REQ_DONE {remote} {rawPath} in {dt:F1} ms");
        }
    }

    private static string CanonicalKey(List<string> files)
    {
        var arr = files.ToArray();
        Array.Sort(arr, StringComparer.OrdinalIgnoreCase);
        return string.Join("&", arr);
    }

    private static bool TryGetFromCache(string key, out CacheEntry? e)
    {
        e = null;
        CacheLock.EnterReadLock();
        try
        {
            return Cache.TryGetValue(key, out e);
        }
        finally
        {
            CacheLock.ExitReadLock();
        }
    }

    private static void PutToCache(string key, CacheEntry e)
    {
        CacheLock.EnterWriteLock();
        try
        {
            Cache[key] = e;
        }
        finally
        {
            CacheLock.ExitWriteLock();
        }
    }


    private static void ClearCacheAll()
    {
        CacheLock.EnterWriteLock();
        try { Cache.Clear(); }
        finally { CacheLock.ExitWriteLock(); }
        Log("CACHE CLEAR: ALL");
    }

    private static int ClearCacheByFile(string file)
    {
        var toRemove = new List<string>();

        CacheLock.EnterReadLock();
        try
        {
            foreach (var k in Cache.Keys)
            {
                var parts = k.Split('&');
                foreach (var p in parts)
                {
                    if (string.Equals(p, file, StringComparison.OrdinalIgnoreCase))
                    {
                        toRemove.Add(k);
                        break;
                    }
                }
            }
        }
        finally { CacheLock.ExitReadLock(); }

        if (toRemove.Count > 0)
        {
            CacheLock.EnterWriteLock();
            try { foreach (var k in toRemove) Cache.Remove(k); }
            finally { CacheLock.ExitWriteLock(); }
        }

        Log($"CACHE CLEAR: file={file} keys={toRemove.Count}");
        return toRemove.Count;
    }


    private static (string FileName, byte[] Data, string Summary) CreateZip(List<(string Name, string FullPath, long Size)> files)
    {
        var keyMaterial = new StringBuilder();
        long totalSrc = 0;
        foreach (var f in files) { keyMaterial.Append(f.Name).Append('|').Append(f.Size).Append(';'); totalSrc += f.Size; }
        var hash = SHA1.HashData(Encoding.UTF8.GetBytes(keyMaterial.ToString()));
        var hex = BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        var zipName = $"bundle-{hex[..12]}.zip";

        using var ms = new MemoryStream();
        using (var zip = new ZipOutputStream(ms) { IsStreamOwner = false })
        {
            zip.SetLevel(9);
            var buffer = new byte[128 * 1024];

            foreach (var (name, full, _) in files)
            {
                var entry = new ZipEntry(name)
                {
                    DateTime = File.GetLastWriteTime(full),
                    IsUnicodeText = true
                };
                zip.PutNextEntry(entry);

                using (var fs = File.OpenRead(full))
                {
                    int read;
                    while ((read = fs.Read(buffer, 0, buffer.Length)) > 0)
                        zip.Write(buffer, 0, read);
                }

                zip.CloseEntry();
            }

            zip.Finish();
        }

        var data = ms.ToArray();
        var summary = $"ZIP[{files.Count} files, {totalSrc} bytes src] -> {data.Length} bytes";
        return (zipName, data, summary);
    }

    private static void WriteZip(HttpListenerResponse res, CacheEntry e)
    {
        res.StatusCode = 200;
        res.ContentType = e.ContentType;
        res.AddHeader("Content-Disposition", $"attachment; filename=\"{e.FileName}\"");
        res.ContentLength64 = e.Data.Length;
        res.OutputStream.Write(e.Data, 0, e.Data.Length);
    }

    private static void WriteStatus(HttpListenerResponse res, int status, string? headerMsg = null)
    {
        res.StatusCode = status;
        if (!string.IsNullOrEmpty(headerMsg))
            res.AddHeader("X-Message", headerMsg);
        res.ContentLength64 = 0;
    }

    private static void Log(string line)
    {
        var stamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        var msg = $"[{stamp}] {line}";
        Console.WriteLine(msg); 
        lock (LogLock)         
        {
            _logWriter?.WriteLine(msg);
            _logWriter?.Flush();
        }
    }

    private static void LogRequest(string remote, string path, int status, string info)
    {
        Log($"REQ {remote} {path} -> {status} | {info}");
    }

    private static string SafeRemote(HttpListenerRequest req)
    {
        try { return req.RemoteEndPoint?.ToString() ?? "-"; }
        catch { return "-"; }
    }
}
