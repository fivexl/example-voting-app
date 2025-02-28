using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
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
        private bool _disposed;

        public HealthCheck(string dbConnectionString, string redisHost, int redisTimeoutMs = 500)
        {
            _dbConnectionString = dbConnectionString;
            _redisHost = redisHost;
            _redisTimeoutMs = redisTimeoutMs;
            _listener = new HttpListener();
            _cancellationTokenSource = new CancellationTokenSource();
            
            // Only listen on localhost for security
            _listener.Prefixes.Add("http://localhost:8080/health/");
        }

        public void Start()
        {
            _listener.Start();
            Console.WriteLine("Health check running on port 8080");

            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try 
                {
                    var context = _listener.GetContext();
                    HandleHealthCheck(context);
                }
                catch (HttpListenerException) when (_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    // Normal shutdown
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Health check error: {ex.Message}");
                }
            }
        }

        private void HandleHealthCheck(HttpListenerContext context)
        {
            using (var response = context.Response)
            {
                var isHealthy = CheckDatabase() && CheckRedis();
                
                response.StatusCode = isHealthy ? 200 : 500;
                response.ContentType = "application/json";
                
                var status = new { status = isHealthy ? "healthy" : "unhealthy" };
                var json = JsonConvert.SerializeObject(status);
                var buffer = System.Text.Encoding.UTF8.GetBytes(json);
                
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
            }
        }

        private bool CheckDatabase()
        {
            try
            {
                using (var connection = new NpgsqlConnection(_dbConnectionString))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT 1";
                        command.CommandTimeout = 5; // 5 second timeout
                        command.ExecuteNonQuery();
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

        private bool CheckRedis()
        {
            try
            {
                var redis = RedisManager.GetRedis();
                return redis.Ping() < TimeSpan.FromMilliseconds(_redisTimeoutMs);
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
            
            GC.SuppressFinalize(this);
        }
    }

    public class Program
    {
        public static int Main(string[] args)
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
                
                var pgsql = OpenDbConnection(dbConnectionString);
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
                while (true)
                {
                    // Slow down to prevent CPU spike, only query each 100ms
                    Thread.Sleep(100);

                    // Reconnect redis if down
                    var multiplexer = redis.Multiplexer;
                    if (!multiplexer.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis");
                        redis = RedisManager.GetRedis();
                    }
                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection(dbConnectionString);
                        }
                        else
                        { // Normal +1 vote requested
                            UpdateVote(pgsql, vote.voter_id, vote.vote);
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
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

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException ex)
                {
                    Console.Error.WriteLine($"SocketException: Waiting for db - {ex.Message}");
                    Thread.Sleep(1000);
                }
                catch (DbException ex)
                {
                    Console.Error.WriteLine($"DbException: Waiting for db - {ex.Message}");
                    Thread.Sleep(1000);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Exception: Waiting for db - {ex.Message}");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                        id VARCHAR(255) NOT NULL UNIQUE,
                                        vote VARCHAR(255) NOT NULL
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}