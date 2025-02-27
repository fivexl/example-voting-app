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
    public class RedisManager
    {
        private static ConnectionMultiplexer _redisConnection;
        private static readonly object LockObject = new object();

        public static IDatabase GetRedis()
        {
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
    }

    public class HealthCheck
    {
        private readonly string _dbConnectionString;
        private readonly string _redisHost;

        public HealthCheck(string dbConnectionString, string redisHost)
        {
            _dbConnectionString = dbConnectionString;
            _redisHost = redisHost;
        }

        public void Start()
        {
            var listener = new HttpListener();
            listener.Prefixes.Add("http://*:8080/health/");
            listener.Start();
            Console.WriteLine("Health check running on port 8080");

            while (true)
            {
                var context = listener.GetContext();
                var response = context.Response;

                if (CheckDatabase() && CheckRedis())
                {
                    response.StatusCode = 200;
                    var buffer = System.Text.Encoding.UTF8.GetBytes("OK");
                    response.OutputStream.Write(buffer, 0, buffer.Length);
                }
                else
                {
                    response.StatusCode = 500;
                    var buffer = System.Text.Encoding.UTF8.GetBytes("Unhealthy");
                    response.OutputStream.Write(buffer, 0, buffer.Length);
                }

                response.Close();
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
                return redis.Ping() < TimeSpan.FromMilliseconds(100);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Redis health check failed: {e.Message}");
                return false;
            }
        }
    }
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                var dbHost = Environment.GetEnvironmentVariable("DB_HOST") ?? "db";
                var dbUser = Environment.GetEnvironmentVariable("DB_USER") ?? "postgres";
                var dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "postgres";
                var dbPassword = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "postgres";
                var dbPort = Environment.GetEnvironmentVariable("DB_PORT") ?? "5432";
                var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";

                var dbConnectionString = $"Host={dbHost};Username={dbUser};Password={dbPassword};Database={dbName};Port={dbPort};SslMode=Require;Trust Server Certificate=true"; // TODO: Use SSL
                
                var pgsql = OpenDbConnection(dbConnectionString);
                var redis = RedisManager.GetRedis();

                var healthCheck = new HealthCheck(dbConnectionString, redisHost);
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
                    if (!_redisConnection.IsConnected)
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

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround https://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
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