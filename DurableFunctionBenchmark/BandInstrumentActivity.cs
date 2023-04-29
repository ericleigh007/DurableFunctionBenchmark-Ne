using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using doc = Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Diagnostics;

namespace DurableFunctionBenchmark
{
    public class BandInstrumentActivity
    {
        [FunctionName(nameof(BandInstrumentActivity))]
        public async Task<InstrumentActivityOutput> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            var input = context.GetInput<InstrumentActivityInput>();
            var itemCount = input.ItemCount;
            var useBulk = input.UseBulk;
            var documentSize = input.DocumentSize;

            // for now, no real work
            //            await Task.Delay(input.DelayTime);

            var sw = Stopwatch.StartNew();

            int retriesAttempted = 0;
            TimeSpan retryTimeSpan = TimeSpan.FromSeconds(0);

            var docList = new List<BenchmarkDocument>();

            int successCount = 0;
            if (documentSize == 0)
            {
                successCount = itemCount;
            }

            for (int i = 0; i < itemCount; i++)
            {
                var partKey = input.RunId;
                if (input.UseMixedPartitionKey)
                {
                    partKey = Guid.NewGuid().ToString();
                }

                docList.Add( new BenchmarkDocument()
                {
                    ActivityId = context.InstanceId,
                    id = Guid.NewGuid().ToString(),
                    RunId = input.RunId,
                    partitionKey = partKey,
                    ActivityName = context.Name,
                    OrchestratorId = input.SubOrchestratorId,
                    ActivityNumber = input.ActivityNumber,
                    ItemNumber = i,
                    OrchestratorQueueTime = input.OrchestratorQueueTime,
                    ActivityQueueTime = input.ActivityQueueTime,
                    OrchestratorDequeueDelay = input.ActivityQueueTime - input.OrchestratorQueueTime,
                    ActivityDequeueDelay = DateTime.UtcNow - input.ActivityQueueTime,
                    OrchestratorNumber = input.SubOrchestratorNumber,
                    StartTime = input.RunStartTime,
                    EndTime = DateTime.UtcNow,
                    TestParameters = input.TestParameters,
                    TestDescription = input.TestDescription,
                    DocumentDescription = $"Orch:{input.SubOrchestratorNumber}:Activity:{input.ActivityNumber}",
                    ScratchString = "this is reserved for putting in a variable length string based on loading criteria",
                });
            }

            if (CosmosContainer.Container is null)
            {
                log.LogWarning($"Singleton Cosmos client initialization in activity");
                await CosmosContainer.InitializeAsync(useBulk);
            }

            var taskList = new List<Task>();
            for (int i = 0; i < itemCount; i++)
            {
                var doc = docList[i];
                log.LogDebug($"{context.Name} starting Orch:{input.SubOrchestratorNumber} Act:{input.ActivityNumber} Item:{i}");

                while (input.DocumentSize > 0)
                {
                    try
                    {
                        await CosmosContainer.Container.UpsertItemAsync<BenchmarkDocument>(doc);
                        successCount++;

                        break;
                    }
                    catch (CosmosException cx) when (cx.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        retriesAttempted++;
                        var retryWait = Utils.GetRetryWait(cx.RetryAfter.Value);
                        retryTimeSpan += TimeSpan.FromMilliseconds(retryWait);

                        doc.CosmosUpsertRetries = retriesAttempted;
                        doc.CosmosUpsertRetryTime = retryTimeSpan;
                            
                        await Task.Delay(retryWait);
                    }
                    catch(CosmosException cx)
                    {
                        log.LogError($"COSMOSEXCEPTION: cosmos other exception {doc.partitionKey}:{doc.id} \n {cx.Message}");
                        // unrecoverable, bail
                        break;
                    }
                }
            }

            await Task.WhenAll(taskList);

            if (retriesAttempted > 0)
            {
                log.LogDebug("UPSERTRETRY: Upserts successful after {retries} retries and {retryTimeSpan} total retry time",
                    retriesAttempted, retryTimeSpan);
            }

            if(successCount == 0)
            {
                log.LogError($"{nameof(BandInstrumentActivity)} did not successfullly upsert benchmark document\n"
                       + $"  for {input.RunId} {input.SubOrchestratorNumber} {input.ActivityNumber}");
            }

            sw.Stop();

            var returnObject = new InstrumentActivityOutput()
            {
                SubOrchestratorNumber = input.SubOrchestratorNumber,
                ActivityNumber = input.ActivityNumber,
                ItemCount = input.ItemCount,
                DocumentSize = input.DocumentSize,
                OutputQueueTime = DateTime.UtcNow,
                ProcessingClockTime = sw.Elapsed,
                RetryCount = retriesAttempted,
                OrchestratorDequeueDelay = docList[0].OrchestratorDequeueDelay,
                ActivityDequeueDelay = docList[0].ActivityDequeueDelay,
                SuccessCount = successCount,
            };

            return returnObject;
        }
    }
}
