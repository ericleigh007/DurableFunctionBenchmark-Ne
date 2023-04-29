using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection.Metadata;
using System.Threading.Tasks;
using doc = Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableFunctionBenchmark
{
    public static class QueryOldSubOrchestrator
    {
        [FunctionName(nameof(QueryOldSubOrchestrator))]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var input = context.GetInput<QueryActivityInput>();

            var res = await context.CallActivityAsync<StatisticsDocument>(nameof(QueryCosmosActivity), input);

            res.RunTime = res.EndTime - res.StartTime;
            res.RunId = input.PartitionKey;

            context.SetCustomStatus(res);

            return "Done";
        }
    }
}