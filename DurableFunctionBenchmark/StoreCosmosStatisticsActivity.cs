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
using Newtonsoft.Json.Linq;

namespace DurableFunctionBenchmark
{
    public class StoreCosmosStatisticsActivity
    {
        [FunctionName(nameof(StoreCosmosStatisticsActivity))]
        public async Task<bool> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            var input = context.GetInput<StatisticsDocument>();

            if(CosmosContainer.Container is null)
            {
                await CosmosContainer.InitializeAsync(false);
            }

            int retriesAttempted = 0;
            TimeSpan retryTimeSpan = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    await CosmosContainer.Container.UpsertItemAsync<StatisticsDocument>(input);
                    break;
                }
                catch (CosmosException cx) when (cx.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    retriesAttempted++;
                    retryTimeSpan += cx.RetryAfter.Value;

                    await Task.Delay(Utils.GetRetryWait(cx.RetryAfter.Value));
                }
                catch (CosmosException cx)
                {
                    log.LogError($"COSMOSSEXCEPTION: cosmos other exception writing stats {input.partitionKey}:{input.id} \n {cx.Message}");
                    break;
                }
            }

            if (retriesAttempted > 0)
            {
                log.LogWarning("STATISTICSRETRY: statistics document write successful after {retries} retries and {retryTimeSpan} total retry time",
                    retriesAttempted, retryTimeSpan);
            }

            return true;
        }
    }
}
