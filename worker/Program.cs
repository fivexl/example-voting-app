using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class RedisManager : IDisposable
    {
        private static ConnectionMultiplexer _redisConnection;
        private static readonly object LockObject = new object();
        private static volatile bool _disposed;

        public static IDatabase GetRedis()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(RedisManager));
            
            if (_redisConnection == null || !_redisConnection.IsConnected)
            {
                lock (LockObject)
                {
                    if (_redisConnection == null || !_redisConnection.IsConnected)
                    {
                        _redisConnection = OpenRedisConnection();
                    }
                }
            }
            return _redisConnection.GetDatabase();
        }

        private static ConnectionMultiplexer OpenRedisConnection()
        {
            var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";
            var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
            var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD");
            var redisDb = Environment.GetEnvironmentVariable("REDIS_DB") ?? "0";
            var redisSocketTimeout = Environment.GetEnvironmentVariable("REDIS_SOCKET_TIMEOUT") ?? "5000";

            var configOptions = new ConfigurationOptions
            {
                EndPoints = { $"{redisHost}:{redisPort}" },
                ConnectTimeout = int.Parse(redisSocketTimeout),
                AbortOnConnectFail = false,
                DefaultDatabase = int.Parse(redisDb),
            };

            if (!string.IsNullOrEmpty(redisPassword))
            {
                configOptions.Password = redisPassword;
            }

            while (true)
            {
                try
                {
                    Console.WriteLine($"Connecting to Redis at {redisHost}:{redisPort}...");
                    return ConnectionMultiplexer.Connect(configOptions);
                }
                catch (RedisConnectionException)
                {
                    Console.WriteLine("Redis connection failed. Retrying...");
                    Thread.Sleep(1000);
                }
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            lock (LockObject)
            {
                if (_disposed) return;
                
                if (_redisConnection != null)
                {
                    _redisConnection.Close();
                    _redisConnection.Dispose();
                    _redisConnection = null;
                }
                _disposed = true;
            }
        }
    }

    public class HealthCheck : IDisposable
    {
        private readonly string _dbConnectionString;
        private readonly string _redisHost;
        private readonly HttpListener _listener;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly int _redisTimeoutMs;
        private readonly SemaphoreSlim _healthCheckLock;
        private bool _disposed;
        private DateTime _lastCheck;
        private bool _lastHealthStatus;
        private static readonly TimeSpan HealthCacheDuration = TimeSpan.FromSeconds(5);

        public HealthCheck(string dbConnectionString, string redisHost, int redisTimeoutMs = 500)
        {
            _dbConnectionString = dbConnectionString;
            _redisHost = redisHost;
            _redisTimeoutMs = redisTimeoutMs;
            _listener = new HttpListener();
            _cancellationTokenSource = new CancellationTokenSource();
            _healthCheckLock = new SemaphoreSlim(1, 1);
            
            // Only listen on localhost for security
            _listener.Prefixes.Add("http://localhost:8080/health/");
        }

        public async void Start()
        {
            _listener.Start();
            Console.WriteLine("Health check running on port 8080");

            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try 
                {
                    var context = await _listener.GetContextAsync();
                    _ = HandleHealthCheckAsync(context); // Fire and forget, but with error handling
                }
                catch (HttpListenerException) when (_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    // Normal shutdown
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Health check error: {ex.Message}");
                    await Task.Delay(1000, _cancellationTokenSource.Token); // Back off on errors
                }
            }
        }

        private async Task HandleHealthCheckAsync(HttpListenerContext context)
        {
            try
            {
                using (var response = context.Response)
                {
                    var isHealthy = await CheckHealthAsync();
                    
                    response.StatusCode = isHealthy ? 200 : 500;
                    response.ContentType = "application/json";
                    
                    var status = new { 
                        status = isHealthy ? "healthy" : "unhealthy",
                        timestamp = DateTime.UtcNow
                    };
                    var json = JsonConvert.SerializeObject(status);
                    var buffer = System.Text.Encoding.UTF8.GetBytes(json);
                    
                    response.ContentLength64 = buffer.Length;
                    await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling health check request: {ex.Message}");
            }
        }

        private async Task<bool> CheckHealthAsync()
        {
            // Use cached health status if within cache duration
            if (DateTime.UtcNow - _lastCheck < HealthCacheDuration)
            {
                return _lastHealthStatus;
            }

            // Ensure only one health check runs at a time
            await _healthCheckLock.WaitAsync();
            try
            {
                // Double-check cache after acquiring lock
                if (DateTime.UtcNow - _lastCheck < HealthCacheDuration)
                {
                    return _lastHealthStatus;
                }

                var dbTask = CheckDatabaseAsync();
                var redisTask = CheckRedisAsync();

                await Task.WhenAll(dbTask, redisTask);
                
                _lastHealthStatus = dbTask.Result && redisTask.Result;
                _lastCheck = DateTime.UtcNow;
                
                return _lastHealthStatus;
            }
            finally
            {
                _healthCheckLock.Release();
            }
        }

        private async Task<bool> CheckDatabaseAsync()
        {
            try
            {
                using (var connection = new NpgsqlConnection(_dbConnectionString))
                {
                    await connection.OpenAsync();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT 1";
                        command.CommandTimeout = 5; // 5 second timeout
                        await command.ExecuteNonQueryAsync();
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Database health check failed: {e.Message}");
                return false;
            }
        }

        private async Task<bool> CheckRedisAsync()
        {
            try
            {
                var redis = RedisManager.GetRedis();
                var pingResult = await redis.PingAsync();
                return pingResult < TimeSpan.FromMilliseconds(_redisTimeoutMs);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Redis health check failed: {e.Message}");
                return false;
            }
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
            _listener.Stop();
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            _disposed = true;
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            _listener.Close();
            _healthCheckLock.Dispose();
            
            GC.SuppressFinalize(this);
        }
    }

    public class Program
    {
        private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan NoVoteDelay = TimeSpan.FromSeconds(1);
        private static readonly int MaxRetries = 5;

        public static async Task<int> Main(string[] args)
        {
            HealthCheck healthCheck = null;
            try
            {
                var dbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "db";
                var dbUser = Environment.GetEnvironmentVariable("DB_USER") ?? "postgres";
                var dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "postgres";
                var dbPassword = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "postgres";
                var dbPort = Environment.GetEnvironmentVariable("DB_PORT") ?? "5432";
                var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";

                var dbConnectionString = $"Host={dbHost};Username={dbUser};Password={dbPassword};Database={dbName};Port={dbPort};SslMode=Require;Trust Server Certificate=true";
                
                var pgsql = await OpenDbConnectionAsync(dbConnectionString);
                var redis = RedisManager.GetRedis();

                healthCheck = new HealthCheck(dbConnectionString, redisHost);
                var healthCheckThread = new Thread(healthCheck.Start);
                healthCheckThread.IsBackground = true;
                healthCheckThread.Start();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "" };
                var retryCount = 0;

                while (true)
                {
                    try
                    {
                        // Reconnect redis if down
                        var multiplexer = redis.Multiplexer;
                        if (!multiplexer.IsConnected)
                        {
                            Console.WriteLine("Reconnecting Redis");
                            redis = RedisManager.GetRedis();
                            await Task.Delay(RetryDelay);
                            continue;
                        }

                        // Process votes
                        string json = await redis.ListLeftPopAsync("votes");
                        if (json != null)
                        {
                            var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                            Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                            
                            // Reconnect DB if down
                            if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                            {
                                Console.WriteLine("Reconnecting DB");
                                pgsql = await OpenDbConnectionAsync(dbConnectionString);
                                continue;
                            }

                            await UpdateVoteAsync(pgsql, vote.voter_id, vote.vote);
                            retryCount = 0; // Reset retry count on successful operation
                        }
                        else
                        {
                            await keepAliveCommand.ExecuteNonQueryAsync();
                            await Task.Delay(NoVoteDelay); // Longer delay when no votes to process
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Error processing vote: {ex.Message}");
                        retryCount++;
                        
                        if (retryCount > MaxRetries)
                        {
                            throw new Exception($"Failed to process votes after {MaxRetries} retries", ex);
                        }
                        
                        await Task.Delay(RetryDelay * retryCount); // Exponential backoff
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
            finally
            {
                healthCheck?.Dispose();
            }
        }

        private static async Task<NpgsqlConnection> OpenDbConnectionAsync(string connectionString)
        {
            NpgsqlConnection connection = null;
            var retryCount = 0;

            while (retryCount <= MaxRetries)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    await connection.OpenAsync();
                    
                    // Initialize the database
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                                id VARCHAR(255) NOT NULL UNIQUE,
                                                vote VARCHAR(255) NOT NULL
                                            )";
                        await command.ExecuteNonQueryAsync();
                    }
                    
                    Console.WriteLine("Connected to db");
                    return connection;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount > MaxRetries)
                    {
                        throw new Exception($"Failed to connect to database after {MaxRetries} retries", ex);
                    }
                    
                    Console.Error.WriteLine($"Database connection attempt {retryCount} failed: {ex.Message}");
                    await Task.Delay(RetryDelay * retryCount); // Exponential backoff
                }
                finally
                {
                    if (connection?.State != System.Data.ConnectionState.Open)
                    {
                        connection?.Dispose();
                    }
                }
            }

            throw new Exception("Failed to connect to database"); // Should never reach here
        }

        private static async Task UpdateVoteAsync(NpgsqlConnection connection, string voterId, string vote)
        {
            using (var command = connection.CreateCommand())
            {
                try
                {
                    command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                    command.Parameters.AddWithValue("@id", voterId);
                    command.Parameters.AddWithValue("@vote", vote);
                    await command.ExecuteNonQueryAsync();
                }
                catch (DbException)
                {
                    command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}