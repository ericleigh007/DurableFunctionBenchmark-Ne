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
    public class GetCosmosThroughputActivity
    {
        [FunctionName(nameof(GetCosmosThroughputActivity))]
        public async Task<int> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            if (CosmosContainer.Container is null)
            {
                await CosmosContainer.InitializeAsync(false);
            }

            int? tp = null; // default to not available
            try
            {
                tp = await CosmosContainer.Container.ReadThroughputAsync();
            }
            catch(CosmosException cx)
            {
                log.LogWarning($"Error obtaining throughput - returned 0");

                tp = 0;
            }

            return (int) tp;
        }
    }
}
