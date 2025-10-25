using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

internal static class Program
{
    private const string Prefix = "http://localhost:5051/"; 
    private static readonly string LogsDir = Path.Combine(AppContext.BaseDirectory, "logs");

    private static IDisposable? _serverConn;

    private static async Task Main()
    {
        Directory.CreateDirectory(LogsDir);
        var logger = new Logger(Path.Combine(LogsDir, $"analyzer-{DateTime.UtcNow:yyyy-MM-dd}.log"));

        using var server = new RxHttpServer(Prefix, logger);
        server.Start();

        var httpClient = CreateGitHubClient();

        var maxConcurrent = Math.Max(2, Environment.ProcessorCount);
        var pipeline =
            server.Requests
                  .ObserveOn(TaskPoolScheduler.Default)
                  .Select(ctx => Observable.FromAsync(() => HandleRequest(ctx, httpClient, logger)))
                  .Merge(maxConcurrent)
                  .Timeout(TimeSpan.FromSeconds(30))
                  .Catch<Unit, Exception>(ex =>
                  {
                      logger.Error($"Pipeline fatal: {ex}");
                      return Observable.Return(Unit.Default);
                  })
                  .Publish();

        _serverConn = pipeline.Connect();

        logger.Info($"Listening on {Prefix}");
        Console.WriteLine("GET /analyze?topic=react or /analyze?topic=react&topic=dotnet");
        Console.WriteLine("Press ENTER to stop...");
        Console.ReadLine();

        _serverConn.Dispose();
        server.Stop();
        httpClient.Dispose();
    }

    private static HttpClient CreateGitHubClient()
    {
        var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.All
        };
        var client = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://api.github.com/")
        };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("RxGitTopicAnalyzer/1.0 (+https://github.com/)");
        client.DefaultRequestHeaders.Accept.ParseAdd("application/vnd.github+json");
        client.DefaultRequestHeaders.Add("X-GitHub-Api-Version", "2022-11-28");

        var token = Environment.GetEnvironmentVariable("GITHUB_TOKEN");
        if (!string.IsNullOrWhiteSpace(token))
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

        return client;
    }

    private static async Task<Unit> HandleRequest(HttpListenerContext ctx, HttpClient http, Logger logger)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var remote = ctx.Request.RemoteEndPoint?.ToString() ?? "unknown";
        var raw = ctx.Request.RawUrl ?? "/";
        var method = ctx.Request.HttpMethod;

        try
        {
            if (!string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase))
            {
                await WriteText(ctx, 405, "Only GET is supported\n");
                logger.Warn($"{remote} {method} {raw} -> 405");
                return Unit.Default;
            }

            var path = ctx.Request.Url?.AbsolutePath ?? "/";
            if (!path.Equals("/analyze", StringComparison.OrdinalIgnoreCase))
            {
                await WriteText(ctx, 404, "Use /analyze?topic=react[&topic=dotnet]\n");
                logger.Warn($"{remote} {method} {raw} -> 404");
                return Unit.Default;
            }

            var query = ctx.Request.Url?.Query ?? "";
            var topics = ParseTopics(query);
            if (topics.Length == 0)
            {
                await WriteText(ctx, 400, "Missing topic parameter. Example: /analyze?topic=react\n");
                logger.Warn($"{remote} {method} {raw} -> 400 no-topic");
                return Unit.Default;
            }

            var topicObs = topics.ToObservable();
            var results = new Dictionary<string, List<RepoDto>>(StringComparer.OrdinalIgnoreCase);
            var maxConcurrent = Math.Min(4, Math.Max(2, Environment.ProcessorCount));

            var rateInfo = new List<RateLimitInfo>();

            await topicObs
                .Select(t => Observable.FromAsync(async () =>
                {
                    var (repos, rate) = await FetchTopic(http, t);
                    lock (results)
                    {
                        results[t] = repos;
                        if (rate is not null) rateInfo.Add(rate);
                    }
                }))
                .Merge(maxConcurrent)
                .DefaultIfEmpty()
                .LastOrDefaultAsync();

            sw.Stop();

            var payload = new
            {
                topics = results.Select(kv => new { topic = kv.Key, repositories = kv.Value }).ToArray(),
                rate_limits = rateInfo.Select(r => new
                {
                    r.Limit,
                    r.Remaining,
                    r.ResetUtc
                }).ToArray(),
                elapsed_ms = sw.ElapsedMilliseconds
            };

            await WriteJson(ctx, 200, payload);

            logger.Info($"{remote} {method} {raw} -> 200 topics={topics.Length} elapsed={sw.ElapsedMilliseconds}ms");
        }
        catch (Exception ex)
        {
            try { await WriteText(ctx, 500, "Internal error\n"); } catch { }
            logger.Error($"Unhandled {remote} {raw}: {ex}");
        }
        finally
        {
            SafeClose(ctx);
        }

        return Unit.Default;
    }

    private static string[] ParseTopics(string query)
    {
        if (string.IsNullOrEmpty(query)) return Array.Empty<string>();
        var parts = System.Web.HttpUtility.ParseQueryString(query);
        var list = new List<string>();
        foreach (var key in parts.AllKeys ?? Array.Empty<string>())
        {
            if (!string.Equals(key, "topic", StringComparison.OrdinalIgnoreCase)) continue;
            var values = parts.GetValues(key);
            if (values == null) continue;
            foreach (var v in values)
            {
                if (string.IsNullOrWhiteSpace(v)) continue;
                foreach (var s in v.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    list.Add(s);
            }
        }
        return list.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static async Task<(List<RepoDto>, RateLimitInfo?)> FetchTopic(HttpClient http, string topic)
    {
        var url = $"search/repositories?q=topic:{Uri.EscapeDataString(topic)}&sort=stars&order=desc&per_page=25&page=1";
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        using var resp = await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead);
        var rate = ParseRate(resp.Headers);

        if (!resp.IsSuccessStatusCode)
        {
            var errTxt = await resp.Content.ReadAsStringAsync();
            return (new List<RepoDto> {
                RepoDto.Error($"GitHub error {((int)resp.StatusCode)}: {Truncate(errTxt, 300)}")
            }, rate);
        }

        var json = await resp.Content.ReadFromJsonAsync<SearchResponse>(new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        var items = json?.Items ?? Array.Empty<SearchItem>();

        var mapped = items.Select(i => new RepoDto
        {
            full_name = i.Full_Name,
            stars = i.Stargazers_Count,
            size_kb = i.Size,          
            forks = i.Forks_Count,
            html_url = i.Html_Url
        }).ToList();

        return (mapped, rate);
    }

    private static RateLimitInfo? ParseRate(System.Net.Http.Headers.HttpResponseHeaders h)
    {
        try
        {
            int limit = h.TryGetValues("X-RateLimit-Limit", out var l) ? int.Parse(l.First()) : 0;
            int remaining = h.TryGetValues("X-RateLimit-Remaining", out var r) ? int.Parse(r.First()) : 0;
            long resetEpoch = h.TryGetValues("X-RateLimit-Reset", out var re) ? long.Parse(re.First()) : 0;
            var resetUtc = resetEpoch > 0 ? DateTimeOffset.FromUnixTimeSeconds(resetEpoch).UtcDateTime : (DateTime?)null;

            return new RateLimitInfo { Limit = limit, Remaining = remaining, ResetUtc = resetUtc };
        }
        catch { return null; }
    }

    private static string Truncate(string s, int max)
        => s.Length <= max ? s : s.Substring(0, max) + "...";

    private static async Task WriteText(HttpListenerContext ctx, int code, string text)
    {
        ctx.Response.StatusCode = code;
        ctx.Response.ContentType = "text/plain; charset=utf-8";
        var buf = Encoding.UTF8.GetBytes(text);
        ctx.Response.ContentLength64 = buf.Length;
        await ctx.Response.OutputStream.WriteAsync(buf, 0, buf.Length);
        ctx.Response.OutputStream.Close();
    }

    private static async Task WriteJson(HttpListenerContext ctx, int code, object payload)
    {
        ctx.Response.StatusCode = code;
        ctx.Response.ContentType = "application/json; charset=utf-8";
        var opts = new JsonSerializerOptions { WriteIndented = true };
        var json = JsonSerializer.Serialize(payload, opts);
        var buf = Encoding.UTF8.GetBytes(json);
        ctx.Response.ContentLength64 = buf.Length;
        await ctx.Response.OutputStream.WriteAsync(buf, 0, buf.Length);
        ctx.Response.OutputStream.Close();
    }

    private static void SafeClose(HttpListenerContext ctx)
    {
        try { ctx.Response.OutputStream?.Dispose(); } catch { }
        try { ctx.Response?.Close(); } catch { }
    }


    private sealed class RateLimitInfo
    {
        public int Limit { get; set; }
        public int Remaining { get; set; }
        public DateTime? ResetUtc { get; set; }
    }

    private sealed class SearchResponse
    {
        [JsonPropertyName("total_count")] public int Total_Count { get; set; }
        [JsonPropertyName("items")] public SearchItem[] Items { get; set; } = Array.Empty<SearchItem>();
    }

    private sealed class SearchItem
    {
        [JsonPropertyName("full_name")] public string Full_Name { get; set; } = "";
        [JsonPropertyName("stargazers_count")] public int Stargazers_Count { get; set; }
        [JsonPropertyName("forks_count")] public int Forks_Count { get; set; }
        [JsonPropertyName("size")] public int Size { get; set; } 
        [JsonPropertyName("html_url")] public string Html_Url { get; set; } = "";
    }

    private sealed class RepoDto
    {
        public string? full_name { get; set; }
        public int? stars { get; set; }
        public int? size_kb { get; set; }
        public int? forks { get; set; }
        public string? html_url { get; set; }
        public string? error { get; set; }

        public static RepoDto Error(string msg) => new RepoDto { error = msg };
    }


    private sealed class RxHttpServer : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Subject<HttpListenerContext> _requests = new();
        private readonly Logger _logger;
        private bool _running;

        public IObservable<HttpListenerContext> Requests => _requests.AsObservable();

        public RxHttpServer(string prefix, Logger logger)
        {
            _logger = logger;
            _listener.Prefixes.Add(prefix);
        }

        public void Start()
        {
            if (_running) return;
            _listener.Start();
            _running = true;
            Task.Run(AcceptLoop);
        }

        private async Task AcceptLoop()
        {
            while (_running)
            {
                HttpListenerContext? ctx = null;
                try
                {
                    ctx = await _listener.GetContextAsync();
                    _requests.OnNext(ctx);
                }
                catch (HttpListenerException hlex) when (hlex.ErrorCode == 995)
                {
                    break;
                }
                catch (ObjectDisposedException) { break; }
                catch (Exception ex)
                {
                    _logger.Error($"Accept loop: {ex}");
                    if (ctx is not null)
                    {
                        try { ctx.Response.StatusCode = 500; ctx.Response.Close(); } catch { }
                    }
                }
            }
            _requests.OnCompleted();
        }

        public void Stop()
        {
            if (!_running) return;
            _running = false;
            try { _listener.Close(); } catch { }
        }

        public void Dispose() => Stop();
    }


    private sealed class Logger
    {
        private readonly string _path;
        private readonly object _lock = new();

        public Logger(string path) => _path = path;
        public void Info(string msg) => Write("INFO", msg);
        public void Warn(string msg) => Write("WARN", msg);
        public void Error(string msg) => Write("ERROR", msg);

        private void Write(string level, string msg)
        {
            var line = $"{DateTime.UtcNow:O} [{level}] {msg}";
            lock (_lock)
            {
                Console.WriteLine(line);
                File.AppendAllText(_path, line + Environment.NewLine, Encoding.UTF8);
            }
        }
    }
}
