using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using System.Threading;

namespace DurableFunctionBenchmark
{
    public class BandConductorOrchestrator
    {
        [FunctionName(nameof(BandConductorOrchestrator))]
        public async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var Log = context.CreateReplaySafeLogger(log);

            var compressedInput = context.GetInput<CompressedObject<TriggerOrchestratorInput>>();
            if (compressedInput is null)
            {
                Log.LogCritical($"{context.Name} got a null buffer and ignoring");
                return "Null";
            }

            var input = compressedInput.Get<TriggerOrchestratorInput>();

            var directString = input.Direct ? "[Direct Launch]" : "[Indirect Launch]";
            var partitionString = input.UseMixedPartitionKey ? "[Mixed Partition Key]" : "[Fixed Partition Key]";
            var useBulk = input.UseBulk;
            var bulkString = useBulk ? "[Cosmos Bulk container]" : "[Cosmos regular container]";

            var cosmosWaitFraction = input.CosmosWaitFraction;
            var fullContext = $"{context.Name} {context.InstanceId}";

            // this is known good by now
            var compressionLevel = compressedInput.CompressionLevel;

            var runId = input.RunId ?? context.NewGuid().ToString();
            var subOrchestratorCount = input.SubOrchestratorCount >= 0 ? input.SubOrchestratorCount : throw new ArgumentOutOfRangeException(nameof(input.SubOrchestratorCount));
            var activityCount = input.ActivityCount >= 0 ? input.ActivityCount : throw new ArgumentOutOfRangeException(nameof(input.ActivityCount));
            var totalActivities = subOrchestratorCount * activityCount;
            var itemCount = input.ItemCount > 0 ? input.ItemCount : 1;
            var totalItems = totalActivities * itemCount;
            var documentSize = input.DocumentSize;
            var throughput = input.CosmosThroughput;

            var testParameters = input.TestParameters;
            var testDescription = input.TestDescription;

            var documents = input.Documents;

            var payload = input.PayloadSize == 0 ? string.Empty
                                    : Utils.GenerateRandomStringOfLength(input.PayloadSize);

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                RunId = runId,
                ReturnCount = 0,
                ExpectedReturnCount = totalItems,
                Message = $"{testParameters}: Initialized",
                Status = "Running",
                StatisticsDocument = null,
                CosmosThroughput = throughput,
            });

            Log.LogInformation($"{testParameters}");

            DateTime startTime;
            DateTime endTime;

            if (!input.Direct)
            {
                startTime = context.CurrentUtcDateTime;

                var tasks = new List<Task<SubOrchestratorOutput>>();
                for (int i = 1; i <= subOrchestratorCount; i++)
                {
                    tasks.Add(context.CallSubOrchestratorAsync<SubOrchestratorOutput>(
                        nameof(BandSectionSubOrchestrator),
                        CompressedObject<SubOrchestratorInput>.Create(
                            new SubOrchestratorInput()
                        {
                            Documents = documents,
                            RunId = runId,
                            RunStartTime = startTime,
                            OrchestratorQueueTime = context.CurrentUtcDateTime,
                            TestParameters = testParameters,
                            TestDescription = testDescription,
                            SubOrchestratorCount = subOrchestratorCount,
                            SubOrchestratorNumber = i,
                            ActivityCount = activityCount,
                            ItemCount = itemCount,
                            UseMixedPartitionKey = input.UseMixedPartitionKey,
                            UseBulk = useBulk,
                            CosmosWaitFraction = cosmosWaitFraction,
                            DocumentSize = documentSize,
                            Payload = payload,
                        }, compressionLevel)));
                }

                await Task.WhenAll(tasks);

                var grandTotalTotalTasks = tasks.Select(t => t.Result.SuccessCount).Sum();
                var grandMaxRetries = tasks.Select(t => t.Result.MaximumRetries).Max();
                var grandMaxTime = tasks.Select(t => t.Result.MaximumTime).Max();

                var minActivityOutputDequeueDelay = tasks.Select(t => t.Result.MinActivityOutputDequeueDelay).Min();
                var maxActivityOutputDequeueDelay = tasks.Select(t => t.Result.MaxActivityOutputDequeueDelay).Max();
                var minOrchestratorDequeueDelay = tasks.Select(t => t.Result.MinOrchestratorDequeueDelay).Min();
                var maxOrchestratorDequeueDelay = tasks.Select(t => t.Result.MaxOrchestratorDequeueDelay).Max();
                var minActivityDequeueDelay = tasks.Select(t => t.Result.MinActivityDequeueDelay).Min();
                var maxActivityDequeueDelay = tasks.Select(t => t.Result.MaxActivityDequeueDelay).Max();
                var minProcessingClockTime = tasks.Select(t => t.Result.MinProcessingClockTime).Min();
                var maxProcessingClockTime = tasks.Select(t => t.Result.MaxProcessingClockTime).Max();

                var currentTime = context.CurrentUtcDateTime;
                var minOrchestratorOutputDequeueDelay = tasks.Select(t => currentTime - t.Result.OrchestratorOutputQueueTime).Min();
                var maxOrchestratorOutputDequeueDelay = tasks.Select(t => currentTime - t.Result.OrchestratorOutputQueueTime).Max();

                log.LogWarning($"{nameof(BandConductorOrchestrator)} Completed {subOrchestratorCount} orchestrations with {activityCount} activites with {itemCount} items each.\n"
                                     + $"  GTTotal:{grandTotalTotalTasks} GTMaxRetry:{grandMaxRetries} GTRetryTime:{grandMaxTime}");
                
                endTime = context.CurrentUtcDateTime;

                if(input.DocumentSize == 0)
                {
                    // we're not waiting for any documents -- just measuring the run time
                    var runTime = endTime - startTime;
                    var itemsPerSecond = grandTotalTotalTasks / runTime.TotalSeconds;
                    log.LogWarning($"{nameof(BandConductorOrchestrator)} {grandTotalTotalTasks} ({subOrchestratorCount}*{activityCount}*{itemCount}) in {runTime} - {itemsPerSecond} items/sec");

                    var statsDoc = new StatisticsDocument()
                    {
                        RunId = runId,
                        TestDescription = testDescription,
                        TestParameters = testParameters,
                        id = context.NewGuid().ToString(),
                        PayloadSize = payload.Length,
                        DocumentSize = 0,
                        CompressionLevel = compressionLevel,
                        StartTime = startTime,
                        EndTime = endTime,
                        MinActivityDequeueDelay = minActivityDequeueDelay,
                        MaxActivityDequeueDelay = maxActivityDequeueDelay,
                        MinOrchestratorDequeueDelay = minOrchestratorDequeueDelay,
                        MaxOrchestratorDequeueDelay = maxOrchestratorDequeueDelay,
                        ActivityQueueDelayVariance = maxActivityDequeueDelay - minActivityDequeueDelay,
                        OrchestratorQueueDelayVariance = maxOrchestratorDequeueDelay - minOrchestratorDequeueDelay,
                        EnqueueEndTime = endTime,
                        EnqueueStartTime = startTime,
                        EnqueueTime = endTime - startTime,
                        RunTime = endTime - startTime,
                        MinActivityOutputDequeueDelay = minActivityOutputDequeueDelay,
                        MaxActivityOutputDequeueDelay = maxActivityOutputDequeueDelay,
                        MinOrchestratorOutputDequeueDelay = minOrchestratorOutputDequeueDelay,
                        MaxOrchestratorOutputDequeueDelay = maxOrchestratorOutputDequeueDelay,
                        EnqueuedItemsPerSecond = grandTotalTotalTasks / runTime.TotalSeconds,
                        MinRunTime = minProcessingClockTime,
                        ProcessedActivitiesPerSecond = grandTotalTotalTasks / runTime.TotalSeconds,
                        ProcessedItemsPerSecond = grandTotalTotalTasks / runTime.TotalSeconds,
                    };

                    await context.CallActivityAsync(nameof(StoreCosmosStatisticsActivity), statsDoc);

                    context.SetCustomStatus(new QueryOrchestratorStatus()
                    {
                        RunId = runId,
                        ReturnCount = grandTotalTotalTasks,
                        ExpectedReturnCount = totalItems,
                        Status = "Complete",
                        Message = $"RunId {runId}: {subOrchestratorCount} orchestrators launched, with {activityCount} activities each, with {itemCount} items each {itemsPerSecond} items/sec",
                        RunTime = runTime,
                        ItemsPerSecond = itemsPerSecond,
                        MinActivityDequeueDelay = minActivityDequeueDelay,
                        MaxActivityDequeueDelay = maxActivityDequeueDelay,
                        MinOrchestratorDequeueDelay = minOrchestratorDequeueDelay,
                        MaxOrchestratorDequeueDelay = maxOrchestratorDequeueDelay,
                        MinProcessingClockTime = minProcessingClockTime,
                        MaxProcessingClockTime = maxProcessingClockTime,
                        MinActivityOutputDequeueDelay = minActivityOutputDequeueDelay,
                        MaxActivityOutputDequeueDelay = maxActivityOutputDequeueDelay,
                        MinOrchestratorOutputDequeueDelay = minOrchestratorOutputDequeueDelay,
                        MaxOrchestratorOutputDequeueDelay = maxOrchestratorOutputDequeueDelay,
                        StatisticsDocument = statsDoc,
                    });

                    return "Completed all tasks";
                }

                input.LaunchStartTime = startTime;
                input.LaunchEndTime = endTime;

                Log.LogInformation($"{fullContext} all {subOrchestratorCount} orchestrators queuing complete");
            } // not direct, so Bandleader is starting orchestrators

            await DoWaitUsingTimer(context, TimeSpan.FromSeconds(1));

            throughput = await context.CallActivityAsync<int>(nameof(GetCosmosThroughputActivity), null);

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                RunId = runId,
                ReturnCount = 0,
                ExpectedReturnCount = totalItems,
                Status = "Running",
                Message = $"RunId {runId}: {subOrchestratorCount} orchestrators launched - waiting for documents",
                LastQueryTime = 0.0,
                LastQueryCharge = 0.0,
                StatisticsDocument = null,
                CosmosThroughput = throughput,
            }) ;

            // if we're Direct, Bandleader calls the above, and this orchestrator is only used for
            // checking whether we're done
            var tryNumber = 1;

            QueryActivityInput orchInput;
            bool documentsDone = false;
            dynamic result;
            var countQueryString = string.Empty;
            var keyName = string.Empty;
            do
            {
                countQueryString
                    = $"Select value count(1) from c where c.Kind = 'Benchmark' and c.RunId = '{runId}'";

                orchInput = new QueryActivityInput(runId,
                    countQueryString,
                    totalItems);

                Log.LogInformation($"{fullContext} RunId:{runId}, Query {countQueryString}");

                result = await context.CallActivityAsync<dynamic>(nameof(QueryCosmosCountActivity), orchInput);

                int docCount = 0;
                if (result is not null)
                {
                    try
                    {
                        docCount = (int)result?["Documents"]?[0];
                    }
                    catch(Exception ex) 
                    {
                        _ = ex;
                    }

                    throughput = await context.CallActivityAsync<int>(nameof(GetCosmosThroughputActivity), null);

                    try
                    {
                        Log.LogWarning($"{fullContext} {runId} got {docCount} of {totalItems} documents");
                        context.SetCustomStatus(new QueryOrchestratorStatus()
                        {
                            RunId = runId,
                            ReturnCount = docCount,
                            LastQueryCharge = (double)result["RUCharge"],
                            LastQueryTime = (double)((TimeSpan)result["ElapsedTime"]).TotalSeconds,
                            ExpectedReturnCount = totalItems,
                            Status = "Running",
                            Message = $"RunId {runId}: Waiting for documents ({tryNumber}), {docCount} written",
                            StatisticsDocument = null,
                            CosmosThroughput = throughput,
                        });
                    }
                    catch(Exception ex)
                    {
                        _ = ex;
                    }

                    if (docCount >= totalItems)
                    {
                        break;
                    }
                }

                await DoWaitUsingTimer(context, TimeSpan.FromSeconds(3));
                tryNumber++;

            } while (!documentsDone);

            Log.LogWarning($"{fullContext} RunId:{runId} got all {totalItems} documents");

            throughput = await context.CallActivityAsync<int>(nameof(GetCosmosThroughputActivity), null);

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                RunId = runId,
                Status = "Running",
                Message = $"RunId {runId}: Make statistics document from {totalItems} benchmark documents",
                LastQueryCharge = (double)result["RUCharge"],
                LastQueryTime = (double)((TimeSpan)result["ElapsedTime"]).TotalSeconds,
                StatisticsDocument = null,
                ExpectedReturnCount = totalItems,
                ReturnCount = totalItems,
                CosmosThroughput = throughput,
            });

            var statsQueryString =                 
                $"Select "
                + $"min(c.StartTime) as StartTime, "
                + $"min(c.EndTime) as MinEndTime, "
                + $"max(c.EndTime) as EndTime, " 
                + $"min(c.OrchestratorDequeueDelay) as MinOrchestratorDequeueDelay, "
                + $"max(c.OrchestratorDequeueDelay) as MaxOrchestratorDequeueDelay, "
                + $"min(c.ActivityDequeueDelay) as MinActivityDequeueDelay, "
                + $"max(c.ActivityDequeueDelay) as MaxActivityDequeueDelay, "
                + $"min(c.CosmosUpsertRetries) as MinCosmosUpsertRetries, "
                + $"max(c.CosmosUpsertRetries) as MaxCosmosUpsertRetries, "
                + $"min(c.CosmosUpsertRetryTime) as MinCosmosUpsertTime, "
                + $"max(c.CosmosUpsertRetryTime) as MaxCosmosUpsertTime "
                + $"from c where c.Kind = 'Benchmark' and c.RunId = '{runId}'";

            orchInput = new QueryActivityInput(runId,
                statsQueryString,                
                totalItems);
            result = await context.CallActivityAsync<dynamic>(nameof(QueryCosmosStatisticsActivity), orchInput);

            StatisticsDocument res = null;
            // sometimes this returns null in out-or-order execution
            if (result is null)
            {
                Log.LogError($"{nameof(BandConductorOrchestrator)} null return from statistics query for {runId}");
                return "error";
            }

            res = result["Documents"]?.ToObject<List<StatisticsDocument>>()[0];
            res.id = context.NewGuid().ToString();

            res.RunId = runId;
            res.RunTime = res.EndTime - res.StartTime;
            res.MinRunTime = res.MinEndTime - res.StartTime;

            res.RunTimeVariance = res.RunTime - res.MinRunTime;

            res.ActivityQueueDelayVariance = res.MaxActivityDequeueDelay - res.MinActivityDequeueDelay;
            res.OrchestratorQueueDelayVariance = res.MaxOrchestratorDequeueDelay - res.MinOrchestratorDequeueDelay;

            res.EnqueueTime = res.EnqueueEndTime - res.EnqueueStartTime;

            res.EnqueueStartTime = input.LaunchStartTime;
            res.EnqueueEndTime = input.LaunchEndTime;
            res.EnqueueTime = input.LaunchEndTime - input.LaunchStartTime;
            res.EnqueuedItemsPerSecond = totalItems / res.EnqueueTime.TotalSeconds;
            res.TestParameters = testParameters;
            res.TestDescription = testDescription;
            res.ProcessedActivitiesPerSecond = totalActivities / res.RunTime.TotalSeconds;
            res.ProcessedItemsPerSecond = totalItems / res.RunTime.TotalSeconds;

            res.CosmosWaitFraction = cosmosWaitFraction;
            res.CosmosThroughput = throughput;

            await context.CallActivityAsync<bool>(nameof(StoreCosmosStatisticsActivity),res);

            throughput = await context.CallActivityAsync<int>(nameof(GetCosmosThroughputActivity), null);

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                RunId = runId,
                // script is checking for this string .. don't change it.
                Status = "Complete",
                Message = "We're done",
                LastQueryCharge = (double)result["RUCharge"],
                LastQueryTime = (double)((TimeSpan)result["ElapsedTime"]).TotalSeconds,
                StatisticsDocument = res,
                ExpectedReturnCount = totalItems,
                ReturnCount = totalItems,
                CosmosThroughput = throughput,
            }) ;

            Log.LogInformation($"{fullContext} RunId:{runId} set final status");

            return "Done";
        }

        public async Task<bool> DoWaitUsingTimer(IDurableOrchestrationContext context, TimeSpan waitTime)
        {
            await context.CreateTimer((context.CurrentUtcDateTime + waitTime), new CancellationTokenSource().Token);
            return true;
        }
    }
}