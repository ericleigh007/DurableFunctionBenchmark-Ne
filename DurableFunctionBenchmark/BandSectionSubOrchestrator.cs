using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace DurableFunctionBenchmark
{
    public class BandSectionSubOrchestrator
    {
        [FunctionName(nameof(BandSectionSubOrchestrator))]
        public async Task<SubOrchestratorOutput> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var Log = context.CreateReplaySafeLogger(log);

            var compressedInput = context.GetInput<CompressedObject<SubOrchestratorInput>>();

            var input = compressedInput.Get<SubOrchestratorInput>();

            var documents = input.Documents;
            var runId = input.RunId;
            var subOrchNo = input.SubOrchestratorNumber;
            var activityCount = input.ActivityCount;
            var itemCount = input.ItemCount;
            var useBulk = input.UseBulk;
            var bulkString = useBulk ? "[Cosmos Bulk container]" : "[Cosmos regular container]";
            var cosmosWaitFraction = input.CosmosWaitFraction;

            var compressionLevel = input.CompressionLevel;

            var documentSize = input.DocumentSize;
            var payLoad = input.Payload;

            Log.LogWarning($"{context.Name} starting orchestrator for RunId:{runId}, #{subOrchNo}, launching {activityCount} activities,\n"
                + $" using {compressedInput.CompressionLevel} compression, factor {compressedInput.CompressionFactor:0.000} in {compressedInput.CompressTime.TotalMilliseconds}mS to compress and {compressedInput.UnCompressTime.TotalMilliseconds}mS to uncompress {compressedInput.UnCompressedLength} length data");

            var tasks = new List<Task<InstrumentActivityOutput>>();
            for (int t = 1; t <= activityCount; t++)
            {
                var fInput
                    = CompressedObject<InstrumentActivityInput>.Create(
                        new InstrumentActivityInput()
                    {
                        Documents = documents,
                        RunId = runId,
                        RunStartTime = input.RunStartTime,
                        OrchestratorQueueTime = input.OrchestratorQueueTime,
                        ActivityQueueTime = context.CurrentUtcDateTime,
                        TestParameters = input.TestParameters,
                        TestDescription = input.TestDescription,
                        SubOrchestratorNumber = subOrchNo,
                        SubOrchestratorId = context.InstanceId,
                        DelayTime = 1,
                        ActivityNumber = t,
                        UseMixedPartitionKey = input.UseMixedPartitionKey,
                        PayLoad = payLoad,
                        DocumentSize = documentSize,
                        UseBulk = useBulk,
                        CosmosWaitFraction = cosmosWaitFraction,
                        ItemCount = itemCount,
                    }, compressionLevel);

                int retryNumber = 0;
                tasks.Add(context.CallActivityWithRetryAsync<InstrumentActivityOutput>(
                    nameof(BandInstrumentActivity),
                    new RetryOptions(TimeSpan.FromSeconds(5), 50)
                    {
                        BackoffCoefficient = 1.2,
                        MaxRetryInterval = TimeSpan.FromSeconds(120),
                        RetryTimeout = TimeSpan.FromMinutes(4),
                        FirstRetryInterval = TimeSpan.FromSeconds(10),
                        Handle = ex =>
                        {
                            retryNumber++;
                            Log.LogWarning($"Exception {retryNumber} from {nameof(BandInstrumentActivity)}. {ex.Message}... ");
                            return true;
                        }
                    },
                    fInput));
            }

            await Task.WhenAll(tasks);

            int maxRetries = tasks.Select(t => t.Result.RetryCount).Max();
            int goodTasks = tasks.Where(t => t.IsCompletedSuccessfully).Count();
            int totalTasks = tasks.Select(t => t.Result.SuccessCount).Sum();

            var currentTime = context.CurrentUtcDateTime;
            var maxTime = TimeSpan.FromSeconds( tasks.Select(t => t.Result.ProcessingClockTime.TotalSeconds).Max());
            var minActivityOutputDequeueDelay = tasks.Select(t => currentTime - t.Result.OutputQueueTime).Min();
            var maxActivityOutputDequeueDelay = tasks.Select(t => currentTime - t.Result.OutputQueueTime).Max();            
            var minOrchestratorDequeueDelay = tasks.Select( t => t.Result.OrchestratorDequeueDelay).Min();
            var maxOrchestratorDequeueDelay = tasks.Select(t => t.Result.OrchestratorDequeueDelay).Max();
            var minActivityDequeueDelay = tasks.Select(t => t.Result.ActivityDequeueDelay).Min();
            var maxActivityDequeueDelay = tasks.Select(t => t.Result.ActivityDequeueDelay).Max();
            var minProcessingClockTime = tasks.Select(t => t.Result.ProcessingClockTime).Min();
            var maxProcessingClockTime = tasks.Select(t => t.Result.ProcessingClockTime).Max();

            if (tasks.Count != totalTasks)
            {
                var badTasks = tasks.Where(t => t.Result.SuccessCount == 0).ToList();
                Log.LogError($"not all tasks marked themselves as completing succesfully -- {badTasks.Count} failed");
                foreach (var bTask in badTasks)
                {
                    var exMsg = bTask?.Exception?.Message ?? "no exception";
                    Log.LogError($"failed: {bTask.Result.ActivityNumber} status msg:{exMsg}");
                }
            }

            Log.LogWarning($"{nameof(BandSectionSubOrchestrator)} completed {goodTasks} of {tasks.Count} tasks for Orchestrator {subOrchNo} with a maximum {maxRetries} throttle retries, max:{maxTime}");

            var returnObject = new SubOrchestratorOutput()
            {
                SubOrchestratorNumber = subOrchNo,
                SuccessCount = totalTasks,
                MaximumRetries = maxRetries,
                MaximumTime = maxTime,
                MinActivityDequeueDelay = minActivityDequeueDelay,
                MaxActivityDequeueDelay = maxActivityDequeueDelay,
                MinOrchestratorDequeueDelay = minOrchestratorDequeueDelay,
                MaxOrchestratorDequeueDelay = maxOrchestratorDequeueDelay,
                MinProcessingClockTime = minProcessingClockTime,
                MaxProcessingClockTime = maxProcessingClockTime,
                MinActivityOutputDequeueDelay = minActivityOutputDequeueDelay,
                MaxActivityOutputDequeueDelay = maxActivityOutputDequeueDelay,
                OrchestratorOutputQueueTime = context.CurrentUtcDateTime,
            };

            return returnObject;
        }
    }
}