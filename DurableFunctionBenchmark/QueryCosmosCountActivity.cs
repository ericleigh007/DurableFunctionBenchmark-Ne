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
    public class QueryCosmosCountActivity
    {
        [FunctionName(nameof(QueryCosmosCountActivity))]
        public async Task<dynamic> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            var input = context.GetInput<QueryActivityInput>();

            if(CosmosContainer.Container is null)
            {
                await CosmosContainer.InitializeAsync(false);
            }

            var statsQuery = Utils.MakeQuery(input.QueryString);

            var docTmp = await Utils.MeasureQueryV3<int>(CosmosContainer.Container, statsQuery);

            if (docTmp.RetriesAttempted > 0)
            {
                log.LogWarning($"query required {docTmp.RetriesAttempted} retries over {docTmp.RetryTimeSpan}"); 
            }
            return docTmp;
        }
    }
}
