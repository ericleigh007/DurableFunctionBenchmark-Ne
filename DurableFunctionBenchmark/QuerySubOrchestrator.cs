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
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using Microsoft.AspNetCore.Mvc;

namespace DurableFunctionBenchmark
{
    public class QuerySubOrchestrator
    {
        [FunctionName(nameof(QuerySubOrchestrator))]
        public async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var Log = context.CreateReplaySafeLogger(log);

            var input = context.GetInput<QueryActivityInput>();

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                ReturnCount = 0,
                Message = $"Initialized",
                StatisticsDocument = null,
            });

            QueryActivityInput orchInput;
            bool documentsDone = false;
            do
            {
                orchInput = new QueryActivityInput(input.PartitionKey,
                    $"Select value count(1) from c where c.Kind = 'Benchmark' and c.partitionKey = '{input.PartitionKey}'",
                    input.TotalItemCount);

                Log.LogInformation("{OrchName} RunId:{RunId}, Query {query}",
                    nameof(QuerySubOrchestrator), input.PartitionKey, input.QueryString);

                var result = await context.CallActivityAsync<int>(nameof(QueryCosmosCountActivity), orchInput);

                documentsDone = result >= input.TotalItemCount;
                if (!documentsDone)
                {
                    context.SetCustomStatus(new QueryOrchestratorStatus()
                    {
                        ReturnCount = result,
                        Message = $"Waiting for documents, {result} written",
                        StatisticsDocument = null,
                    });
                    await context.CallActivityAsync(nameof(DelayActivity), 1000);
                }
            } while (!documentsDone);

            orchInput = new QueryActivityInput(input.PartitionKey,
                $"Select min(c.StartTime) as StartTime, max(c.EndTime) as EndTime from c where c.Kind = 'Benchmark' and c.partitionKey = '{input.PartitionKey}'",
                input.TotalItemCount);
            var res = await context.CallActivityAsync<StatisticsDocument>(nameof(QueryCosmosStatisticsActivity), orchInput);

            res.RunTime = res.EndTime - res.StartTime;
            res.RunId = input.PartitionKey;

            context.SetCustomStatus(new QueryOrchestratorStatus()
            {
                Message = "Complete",
                StatisticsDocument = res,
                ReturnCount = input.TotalItemCount,
            });
            
            return "Done";
        }
    }
}