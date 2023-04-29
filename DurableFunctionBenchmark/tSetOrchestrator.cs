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
    public class WorkActivityInput
    {
        public List<doc.Document> Documents;
        public int ItemNo;
        public int DelayTime;

        public WorkActivityInput( List<doc.Document> documents, int itemNo, int delayTime)
        {
            Documents = documents;
            ItemNo = itemNo;
            DelayTime = delayTime;
        }
    }

    public static class tSetOrchestrator
    {
        private const int parallelCount = 200;

        [FunctionName("tSetOrchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var input = context.GetInput<List<doc.Document>>();
            log.LogInformation($"{context.Name} change feed trigger got {input.Count} items");

            await Task.Delay(1); // a very small load

            var tasks = new List<Task>();
            for (int t = 0; t < parallelCount; t++)
            {
                var fInput = new WorkActivityInput(input, t, 1);
                tasks.Add(context.CallActivityAsync<string>("WorkActivity", fInput));
            }
            var done = Task.WhenAll(tasks);

            Console.WriteLine(done.ToString());

            return "Done";
        }
    }
}