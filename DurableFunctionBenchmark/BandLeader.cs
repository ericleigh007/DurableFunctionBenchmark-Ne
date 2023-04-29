using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using doc = Microsoft.Azure.Documents;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using DurableTask.Core;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using System.Configuration;
using Microsoft.Extensions.Configuration;
using static Microsoft.AspNetCore.Hosting.Internal.HostingApplication;
using System.Security.Permissions;
using System.Reflection.Metadata;
using Microsoft.Identity.Client;
using static System.Net.Mime.MediaTypeNames;

namespace DurableFunctionBenchmark
{
    public class TriggerOrchestratorInput
    {
        public string RunId { get; set; }
        public int SubOrchestratorCount { get; set; }
        public int ActivityCount { get; set; }
        public int ItemCount { get; set; }
        public int PayloadSize { get; set; }
        public int DocumentSize { get; set; } = 100;
        public string TestParameters { get; set; }
        public bool Direct { get; set; }
        public bool UseMixedPartitionKey { get; set; }
        public bool UseBulk { get; set; }
        public DateTime LaunchStartTime { get; set; }
        public DateTime LaunchEndTime { get; set; }

        public bool Debug { get; set; }
        public int CosmosThroughput { get; set; }
    }

    public class SubOrchestratorInput
    {
        public List<JObject> Documents { get; set; }
        public int DocumentSize { get; set; }
        public string RunId { get; set; }
        public DateTime RunStartTime { get; set; }
        public DateTime OrchestratorQueueTime { get; set; }
        public string TestParameters { get; set; }
        public string TestDescription { get; set; }
        public int SubOrchestratorCount { get; set; }
        public int SubOrchestratorNumber { get; set; }
        public int ActivityCount { get; set; }
        public int ItemCount { get; set; }
        public bool UseMixedPartitionKey { get; set; }
        public bool UseBulk { get; set; }
        public string Payload { get; set; }
    }

    public class InstrumentActivityInput
    {
        public List<JObject> Documents { get; set; }
        public string RunId { get; set; }
        public string TestParameters { get; set; }
        public string TestDescription { get; set; }
        public DateTime RunStartTime { get; set; }
        public DateTime OrchestratorQueueTime { get; set; }
        public DateTime ActivityQueueTime { get; set; }
        public string SubOrchestratorId { get; set; }
        public int SubOrchestratorNumber { get; set; }
        public int ItemCount { get; set; }
        public int DocumentSize { get; set; }
        public int ActivityNumber { get; set; }
        public bool UseMixedPartitionKey { get; set; }
        public bool UseBulk { get; set; }
        public int DelayTime { get; set; }
    }

    public class SubOrchestratorOutput
    {
        public int SubOrchestratorNumber { get; set; }
        public int SuccessCount { get; set; }
        public int MaximumRetries { get; set; }
        public TimeSpan MaximumTime { get; set; }
        public TimeSpan MinProcessingClockTime { get; set; }
        public TimeSpan MaxProcessingClockTime { get; set; }
        public TimeSpan MinOrchestratorDequeueDelay { get; set; }
        public TimeSpan MaxOrchestratorDequeueDelay { get; set; }
        public TimeSpan MinActivityDequeueDelay { get; set; }
        public TimeSpan MaxActivityDequeueDelay { get; set; }
        public TimeSpan MinActivityOutputDequeueDelay { get; set; }
        public TimeSpan MaxActivityOutputDequeueDelay { get; set; }
        public DateTime OrchestratorOutputQueueTime { get; set; }
    }

    public class InstrumentActivityOutput
    {
        public int SubOrchestratorNumber { get; set; }
        public int ActivityNumber { get; set; }
        public int ItemCount { get; set; }
        public int DocumentSize { get; set; }
        public int SuccessCount { get; set; }
        public int RetryCount { get; set; }
        public TimeSpan ProcessingClockTime { get; set; }
        public TimeSpan OrchestratorDequeueDelay { get; set; }
        public TimeSpan ActivityDequeueDelay { get; set; }
        public DateTime OutputQueueTime { get; set; }
    }

    public class QueryActivityInput
    {
        public string PartitionKey { get; set; }
        public string QueryString { get; set; }
        public int TotalItemCount { get; set; }

        public QueryActivityInput( string partitionKey, string queryString, int totalItemCount )
        {
            TotalItemCount = totalItemCount;
            PartitionKey = partitionKey;
            QueryString = queryString;
        }
    }

    public static class CosmosContainer
    {
        public static CosmosClient Client { get; set; }
        public static string DatabaseName { get; set; } = null;
        public static string ContainerName { get; set; }
        public static Container Container { get; set; }

        public static string ConnectionString { get; set; } = String.Empty;

        public static async Task<CosmosException> InitializeAsync(bool useBulk)
        {
            var config = new ConfigurationBuilder()
                    .AddEnvironmentVariables()
                    .Build();

            var databaseName = config.GetValue<string>("DurableBenchmark_DatabaseName");
            var containerName = config.GetValue<string>("DurableBenchmark_ContainerName");
            var connectionString = config.GetValue<string>("DurableBenchmark_ConnectionString");

            CosmosContainer.DatabaseName = databaseName;
            CosmosContainer.ContainerName = containerName;
            CosmosContainer.ConnectionString = connectionString;

            var clientOptions = new CosmosClientOptions()
            {
                // TEMPORARILY USING FOR SOMETHING ELSE // AllowBulkExecution = useBulk,
                // using our own retry algorithm
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(0),
            };

            try
            {
                if (useBulk)
                {
                    CosmosContainer.Client = await CosmosClient.CreateAndInitializeAsync(
                        CosmosContainer.ConnectionString,
                        new List<(string db, string container)>()
                        {
                        ( CosmosContainer.DatabaseName, CosmosContainer.ContainerName),
                        },
                        clientOptions);
                }
                else
                {
                    CosmosContainer.Client = new CosmosClient(CosmosContainer.ConnectionString,
                        clientOptions);
                }
            }
            catch (CosmosException cx)
            {
                return cx;
            }

            CosmosContainer.Container = CosmosContainer.Client.GetContainer(CosmosContainer.DatabaseName, CosmosContainer.ContainerName);
            return null;
        }
    }

    public class BenchmarkDocument
    {
        public string partitionKey { get; set; }
        public string id { get; set; }
        public string Kind { get; set; } = "Benchmark";

        public string RunId { get; set; }
        public string ActivityName { get; set; }
        public string OrchestratorId { get; set; }
        public string ActivityId { get; set; }
        public int OrchestratorNumber { get; set; }
        public int ActivityNumber { get; set; }
        public int ItemNumber { get; set; }
        public DateTime OrchestratorQueueTime { get; set; }
        public TimeSpan OrchestratorDequeueDelay { get; set; }
        public DateTime ActivityQueueTime { get; set; }
        public TimeSpan ActivityDequeueDelay { get; set; }
        public int CosmosUpsertRetries { get; set; }
        public TimeSpan CosmosUpsertRetryTime { get; set; }
        public string TestParameters { get; set; }
        public string TestDescription { get; set; }
        public string DocumentDescription { get; set; }
        public string ScratchString { get; set; } = string.Empty;
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }

    public class QueryOrchestratorStatus
    {
        public string Version { get; set; } = "Durable Functions Benchmark-Ne, V0.2, 2023-03-21";
        public string Status { get; set; }
        public string Message { get; set; }
        public string RunId { get; set; }
        public int ExpectedReturnCount { get; set; }
        public int ReturnCount { get; set; }
        public double LastQueryCharge { get; set; }
        public double LastQueryTime { get; set; }
        public TimeSpan RunTime { get; set; }
        public double ItemsPerSecond { get; set; }
        public TimeSpan MinProcessingClockTime { get; set; }
        public TimeSpan MaxProcessingClockTime { get; set; }
        public TimeSpan MinOrchestratorDequeueDelay { get; set; }
        public TimeSpan MaxOrchestratorDequeueDelay { get; set; }
        public TimeSpan MinActivityDequeueDelay { get; set; }
        public TimeSpan MaxActivityDequeueDelay { get; set; }
        public TimeSpan MinActivityOutputDequeueDelay { get; set; }
        public TimeSpan MaxActivityOutputDequeueDelay { get; set; }
        public TimeSpan MinOrchestratorOutputDequeueDelay { get; set; }
        public TimeSpan MaxOrchestratorOutputDequeueDelay { get; set; }
        public int CosmosThroughput { get; set; }
        public StatisticsDocument StatisticsDocument { get; set; }
    }

    public class StatisticsDocument
    {
        public string Kind { get; set; } = "Statistics";
        public string RunId { get; set; }
        public string partitionKey => RunId;
        public string id { get; set; }
        public string TestParameters { get; set; }
        public string TestDescription { get; set; }
        public int PayloadSize { get; set; }
        public int DocumentSize { get; set; }
        public int MinCosmosUpsertRetries { get; set; }
        public int MaxCosmosUpsertRetries { get; set; }
        public TimeSpan MinCosmosUpsertTime { get; set; }
        public TimeSpan MaxCosmosUpsertTime { get; set; }
        public DateTime EnqueueStartTime { get; set; }
        public DateTime EnqueueEndTime { get; set; }
        public TimeSpan EnqueueTime { get; set; }
        public double EnqueuedItemsPerSecond { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime MinEndTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan MinRunTime { get; set; }
        public TimeSpan RunTime { get; set; }
        public TimeSpan RunTimeVariance { get; set; }

        public TimeSpan MaxOrchestratorDequeueDelay { get; set; }
        public TimeSpan MinOrchestratorDequeueDelay { get; set; }
        public TimeSpan MaxActivityDequeueDelay { get; set; }
        public TimeSpan MinActivityDequeueDelay { get; set; }

        public TimeSpan OrchestratorQueueDelayVariance { get; set; }
        public TimeSpan ActivityQueueDelayVariance { get; set; }
        public TimeSpan MinActivityOutputDequeueDelay { get; set; }
        public TimeSpan MaxActivityOutputDequeueDelay { get; set; }
        public TimeSpan MinOrchestratorOutputDequeueDelay { get; set; }
        public TimeSpan MaxOrchestratorOutputDequeueDelay { get; set; }

        public double ProcessedActivitiesPerSecond { get; set; }

        public double ProcessedItemsPerSecond { get; set; }
        public int CosmosThroughput { get; set; }
    }

    public class BandLeader
    {
        [FunctionName("BandLeader")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var json = await req.ReadAsStringAsync();

            log.LogInformation($"{nameof(BandLeader)}, parsing body, {json.Length} characters");

            var input = JsonConvert.DeserializeObject<TriggerOrchestratorInput>(json);

            var runId = input.RunId ?? Guid.NewGuid().ToString();
            var activityCount = input.ActivityCount > 0 ? input.ActivityCount : throw new ArgumentOutOfRangeException(nameof(input.ActivityCount));
            var itemCount = input.ItemCount > 0 ? input.ItemCount : 1;
            var subOrchestratorCount = input.SubOrchestratorCount >= 0 ? input.SubOrchestratorCount : throw new ArgumentOutOfRangeException(nameof(input.SubOrchestratorCount));
            var totalResults = subOrchestratorCount * activityCount * itemCount;
            var documentSize = input.DocumentSize;

            var directMode = input.Direct;
            var directString = directMode ? "[DIRECT launch]" : "[indirect launch through orchestrator]";
            var useMixedPartitionKey = input.UseMixedPartitionKey;
            var partitionKeyString = useMixedPartitionKey ? "[Mixed Partition Key]" : "[Fixed Partition Key]";
            var useBulk = input.UseBulk;
            var bulkString = useBulk ? "[Cosmos Bulk container]" : "[Cosmos regular container]";

            string testParameters = $"Ne: Run:{runId}:OCount:{subOrchestratorCount}:ACount:{activityCount}:ICount{itemCount}, DocSize:{documentSize}, {directString} {bulkString}, Query for {partitionKeyString}";
            if(input.DocumentSize == 0)
            {
                testParameters = $"Ne: Run:{runId}:OCount:{subOrchestratorCount}:ACount:{activityCount}:ICount{itemCount}, {directString} DocSize:{documentSize} NO COSMOS DB";
            }

            var testDescription = string.Empty;

            var docs = new List<JObject>();
            var payload = "big payload in the future";

            await CosmosContainer.InitializeAsync(useBulk);

            Microsoft.Azure.Cosmos.AccountProperties props = null;
            try
            {
                props = await CosmosContainer.Client.ReadAccountAsync();
            }
            catch (CosmosException cx)
            {
                return new ObjectResult($"error connecting with Cosmos {CosmosContainer.DatabaseName} {CosmosContainer.ContainerName}\n "
                + $"Check application settings for databasename, containername and connectionstring\n"
                + $"{cx.Message}");
            }

            var throughput = (int) await CosmosContainer.Container.ReadThroughputAsync();

            log.LogInformation($"{nameof(BandLeader)}, initialialized cosmos DB {CosmosContainer.DatabaseName} container {CosmosContainer.ContainerName}\n"
                 + $"{props.Id} {props.Consistency.DefaultConsistencyLevel} {props.ReadableRegions.First().Name} {props.WritableRegions.First().Name}");

            log.LogInformation($"{nameof(BandLeader)} {testParameters} started creating activities");

            string instanceId = string.Empty;
            var oInput = new TriggerOrchestratorInput();

            if (!input.Direct)
            {
                oInput = new TriggerOrchestratorInput()
                {
                    TestParameters = testParameters,
                    RunId = runId,
                    UseMixedPartitionKey = useMixedPartitionKey,
                    UseBulk = useBulk,
                    Direct = input.Direct,
                    ActivityCount = activityCount,
                    ItemCount = itemCount,
                    SubOrchestratorCount = subOrchestratorCount,
                    DocumentSize = documentSize,
                    PayloadSize = input.PayloadSize,
                    CosmosThroughput = throughput,
                };
                instanceId = await starter.StartNewAsync(nameof(BandConductorOrchestrator), oInput);

                log.LogInformation($"{nameof(BandLeader)} Orchestrator mode -- using {nameof(BandConductorOrchestrator)} to launch {subOrchestratorCount} orchestrators");

                return starter.CreateCheckStatusResponse(req, instanceId);
            }

            if (input.DocumentSize == 0)
            {
                log.LogInformation($"Specify 'Direct = false' and 'DocumentSize = 0' in order to test backend without cosmos");
                return new BadRequestObjectResult("Specify 'Direct = false' and 'DocumentSize = 0' in order to test backend only");
            }

            log.LogInformation($"{nameof(BandLeader)} DIRECT mode -- launching {subOrchestratorCount} orchestrators");

            var startTime = DateTime.UtcNow;

            var tasks = new List<Task<string>>();
            for (int i = 1; i <= subOrchestratorCount; i++)
            {
                tasks.Add(starter.StartNewAsync(
                    nameof(BandSectionSubOrchestrator),
                    new SubOrchestratorInput()
                    {
                        Documents = docs,
                        RunId = runId,
                        RunStartTime = startTime,
                        OrchestratorQueueTime = DateTime.UtcNow,
                        TestParameters = testParameters,
                        TestDescription = testDescription,
                        SubOrchestratorCount = subOrchestratorCount,
                        SubOrchestratorNumber = i,
                        ActivityCount = activityCount,
                        ItemCount = itemCount,
                        UseMixedPartitionKey = useMixedPartitionKey,
                        UseBulk = useBulk,
                        DocumentSize = documentSize,
                        Payload = payload,
                    }));
            }

            await Task.WhenAll(tasks);

            var taskIds = tasks.Select(t => t.Result).ToList();

            var endTime = DateTime.UtcNow;

            log.LogInformation($"{nameof(BandLeader)} all {subOrchestratorCount} orchestrators queuing complete");

            oInput = new TriggerOrchestratorInput()
            {
                TestParameters = testParameters,
                Direct = input.Direct,
                UseMixedPartitionKey = input.UseMixedPartitionKey,
                UseBulk = useBulk,
                ActivityCount = activityCount,
                ItemCount = itemCount,
                RunId = runId,
                DocumentSize = documentSize,
                PayloadSize = input.PayloadSize,
                LaunchStartTime = startTime,
                LaunchEndTime = endTime,
                SubOrchestratorCount = subOrchestratorCount,
                CosmosThroughput = throughput,
            };

            instanceId = await starter.StartNewAsync(nameof(BandConductorOrchestrator), oInput);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
