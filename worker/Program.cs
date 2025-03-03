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
        private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);
        private static readonly int MaxRetries = 5;

        public static IDatabase GetRedis()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(RedisManager));
            
            try
            {
                if (_redisConnection == null || !_redisConnection.IsConnected)
                {
                    lock (LockObject)
                    {
                        if (_redisConnection == null || !_redisConnection.IsConnected)
                        {
                            if (_redisConnection != null)
                            {
                                Console.WriteLine($"Redis connection state: Connected={_redisConnection.IsConnected}, " +
                                                $"ConnectionState={_redisConnection.GetStatus()}, " +
                                                $"Endpoints={string.Join(",", _redisConnection.GetEndPoints().Select(e => e.ToString()))}");
                            }
                            _redisConnection = OpenRedisConnection();
                        }
                    }
                }

                var database = _redisConnection.GetDatabase();
                if (database == null)
                {
                    throw new InvalidOperationException("Failed to get Redis database instance");
                }
                return database;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error getting Redis connection: {ex.GetType().Name} - {ex.Message}");
                Console.Error.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private static ConnectionMultiplexer OpenRedisConnection()
        {
            var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";
            var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
            var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD");
            var redisDb = Environment.GetEnvironmentVariable("REDIS_DB") ?? "0";
            var redisSocketTimeout = Environment.GetEnvironmentVariable("REDIS_SOCKET_TIMEOUT") ?? "5000";

            Console.WriteLine($"Configuring Redis connection to {redisHost}:{redisPort}, DB={redisDb}, Timeout={redisSocketTimeout}ms");

            var configOptions = new ConfigurationOptions
            {
                EndPoints = { $"{redisHost}:{redisPort}" },
                ConnectTimeout = int.Parse(redisSocketTimeout),
                AbortOnConnectFail = false,
                DefaultDatabase = int.Parse(redisDb),
                ConnectRetry = 3,
                SyncTimeout = 5000,
                KeepAlive = 5,
                ReconnectRetryPolicy = new ExponentialRetry(InitialRetryDelay.Milliseconds),
            };

            if (!string.IsNullOrEmpty(redisPassword))
            {
                configOptions.Password = redisPassword;
            }

            var retryCount = 0;
            var delay = InitialRetryDelay;
            Exception lastException = null;

            while (retryCount < MaxRetries)
            {
                try
                {
                    Console.WriteLine($"Connecting to Redis at {redisHost}:{redisPort}... (attempt {retryCount + 1}/{MaxRetries})");
                    var connection = ConnectionMultiplexer.Connect(configOptions);
                    
                    // Log connection state
                    Console.WriteLine($"Redis connection established. State: Connected={connection.IsConnected}, " +
                                    $"ConnectionState={connection.GetStatus()}, " +
                                    $"Endpoints={string.Join(",", connection.GetEndPoints().Select(e => e.ToString()))}");
                    
                    // Verify connection is working
                    var pingResult = connection.GetDatabase().Ping();
                    Console.WriteLine($"Redis ping successful. Response time: {pingResult.TotalMilliseconds}ms");
                    
                    connection.ConnectionFailed += (sender, args) =>
                    {
                        Console.Error.WriteLine($"Redis connection failed. Endpoint: {args.EndPoint}, " +
                                              $"FailureType: {args.FailureType}, " +
                                              $"Exception: {args.Exception?.Message}");
                    };

                    connection.ConnectionRestored += (sender, args) =>
                    {
                        Console.WriteLine($"Redis connection restored. Endpoint: {args.EndPoint}");
                    };

                    connection.ErrorMessage += (sender, args) =>
                    {
                        Console.Error.WriteLine($"Redis error: {args.Message}");
                    };
                    
                    return connection;
                }
                catch (RedisConnectionException ex)
                {
                    lastException = ex;
                    retryCount++;
                    
                    Console.Error.WriteLine($"Redis connection attempt {retryCount} failed:");
                    Console.Error.WriteLine($"  Error Type: {ex.GetType().Name}");
                    Console.Error.WriteLine($"  Message: {ex.Message}");
                    Console.Error.WriteLine($"  Inner Exception: {ex.InnerException?.Message}");
                    
                    if (retryCount >= MaxRetries)
                    {
                        Console.Error.WriteLine("Maximum retry attempts reached. Giving up.");
                        throw new Exception($"Failed to connect to Redis after {MaxRetries} attempts", ex);
                    }

                    Console.WriteLine($"Retrying in {delay.TotalSeconds} seconds...");
                    Thread.Sleep(delay);
                    
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, MaxRetryDelay.TotalMilliseconds));
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Unexpected error while connecting to Redis:");
                    Console.Error.WriteLine($"  Error Type: {ex.GetType().Name}");
                    Console.Error.WriteLine($"  Message: {ex.Message}");
                    Console.Error.WriteLine($"  Stack Trace: {ex.StackTrace}");
                    throw;
                }
            }

            throw new Exception("Failed to connect to Redis", lastException);
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            lock (LockObject)
            {
                if (_disposed) return;
                
                if (_redisConnection != null)
                {
                    try
                    {
                        Console.WriteLine("Closing Redis connection...");
                        _redisConnection.Close();
                        _redisConnection.Dispose();
                        _redisConnection = null;
                        Console.WriteLine("Redis connection closed successfully");
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Error disposing Redis connection: {ex.Message}");
                    }
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
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(2);
        private static readonly int MaxRetries = 5;

        public static async Task<int> Main(string[] args)
        {
            HealthCheck healthCheck = null;
            try
            {
                // Add initial delay to allow DNS and other services to initialize
                await Task.Delay(StartupDelay);
                
                var dbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "db";
                var dbUser = Environment.GetEnvironmentVariable("DB_USER") ?? "postgres";
                var dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "postgres";
                var dbPassword = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "postgres";
                var dbPort = Environment.GetEnvironmentVariable("DB_PORT") ?? "5432";
                var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";

                var dbConnectionString = $"Host={dbHost};Username={dbUser};Password={dbPassword};Database={dbName};Port={dbPort};SslMode=Require;Trust Server Certificate=true";
                
                // Initialize connections sequentially to avoid overwhelming the system
                Console.WriteLine("Initializing database connection...");
                var pgsql = await OpenDbConnectionAsync(dbConnectionString);
                
                Console.WriteLine("Initializing Redis connection...");
                var redis = RedisManager.GetRedis();

                // Start health check after main services are connected
                Console.WriteLine("Starting health check service...");
                healthCheck = new HealthCheck(dbConnectionString, redisHost);
                var healthCheckThread = new Thread(healthCheck.Start);
                healthCheckThread.IsBackground = true;
                healthCheckThread.Start();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";
                keepAliveCommand.CommandTimeout = 5; // 5 second timeout

                var definition = new { vote = "", voter_id = "" };
                var retryCount = 0;

                Console.WriteLine("Worker service started and ready to process votes.");
                
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
            Exception lastException = null;

            Console.WriteLine($"Attempting to connect to database with connection string: {new NpgsqlConnectionStringBuilder(connectionString) { Password = "***" }}");

            while (retryCount <= MaxRetries)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
            
                    // Log connection state changes
                    connection.StateChange += (sender, args) =>
                    {
                        Console.WriteLine($"Database connection state changed from {args.OriginalState} to {args.CurrentState}");
                    };

                    Console.WriteLine($"Opening database connection (attempt {retryCount + 1}/{MaxRetries})...");
                    await connection.OpenAsync();
            
                    // Verify connection is working
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT version()";
                        command.CommandTimeout = 5;
                        var version = await command.ExecuteScalarAsync();
                        Console.WriteLine($"Database connection verified. Server version: {version}");
                    }
            
                    // Initialize the database
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                                id VARCHAR(255) NOT NULL UNIQUE,
                                                vote VARCHAR(255) NOT NULL
                                            )";
                        command.CommandTimeout = 10; // Longer timeout for schema creation
                        await command.ExecuteNonQueryAsync();
                        Console.WriteLine("Database schema initialized successfully");
                    }
            
                    Console.WriteLine($"Database connection established successfully. State: {connection.State}, ProcessID: {connection.ProcessID}");
                    return connection;
                }
                catch (NpgsqlException ex)
                {
                    lastException = ex;
                    retryCount++;
            
                    Console.Error.WriteLine($"Database connection attempt {retryCount} failed:");
                    Console.Error.WriteLine($"  Error Code: {ex.ErrorCode}");
                    Console.Error.WriteLine($"  Severity: {ex.Severity}");
                    Console.Error.WriteLine($"  Message: {ex.Message}");
                    Console.Error.WriteLine($"  Detail: {ex.Detail}");
                    Console.Error.WriteLine($"  Hint: {ex.Hint}");
            
                    if (ex.InnerException != null)
                    {
                        Console.Error.WriteLine($"  Inner Exception: {ex.InnerException.Message}");
                    }
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    retryCount++;
            
                    Console.Error.WriteLine($"Unexpected error while connecting to database:");
                    Console.Error.WriteLine($"  Error Type: {ex.GetType().Name}");
                    Console.Error.WriteLine($"  Message: {ex.Message}");
                    Console.Error.WriteLine($"  Stack Trace: {ex.StackTrace}");
                }
                finally
                {
                    if (connection?.State != System.Data.ConnectionState.Open)
                    {
                        try
                        {
                            await connection?.DisposeAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"Error disposing database connection: {ex.Message}");
                        }
                    }
                }

                if (retryCount > MaxRetries)
                {
                    Console.Error.WriteLine("Maximum database connection retry attempts reached. Giving up.");
                    throw new Exception($"Failed to connect to database after {MaxRetries} attempts", lastException);
                }

                var delay = RetryDelay * retryCount;
                Console.WriteLine($"Retrying database connection in {delay.TotalSeconds} seconds...");
                await Task.Delay(delay);
            }

            throw new Exception("Failed to connect to database", lastException);
        }

        private static async Task UpdateVoteAsync(NpgsqlConnection connection, string voterId, string vote)
        {
            try
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
                    catch (PostgresException ex) when (ex.SqlState == "23505") // Unique violation
                    {
                        Console.WriteLine($"Vote already exists for voter {voterId}, updating...");
                        command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (DbException ex)
                    {
                        Console.Error.WriteLine($"Database error while updating vote:");
                        Console.Error.WriteLine($"  Error Type: {ex.GetType().Name}");
                        Console.Error.WriteLine($"  Message: {ex.Message}");
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Unexpected error while updating vote:");
                Console.Error.WriteLine($"  Error Type: {ex.GetType().Name}");
                Console.Error.WriteLine($"  Message: {ex.Message}");
                Console.Error.WriteLine($"  Stack Trace: {ex.StackTrace}");
                throw;
            }
        }
    }
}