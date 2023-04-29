using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using doc = Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableFunctionBenchmark
{
    internal class WorkActivity
    {
        [FunctionName(nameof(WorkActivity))]
        public async Task<string> Run([ActivityTrigger] IDurableActivityContext context)
        {
            var input = context.GetInput<WorkActivityInput>();

            await Task.Delay(input.DelayTime);

            return $"Done {input.DelayTime}";
        }
    }
}
