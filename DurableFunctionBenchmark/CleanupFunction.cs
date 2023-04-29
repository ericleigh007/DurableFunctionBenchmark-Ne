using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using DurableTask.AzureStorage;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Net.Http;
using System.Collections.Generic;

namespace DurableFunctionBenchmark
{
    public class CleanupFunction
    {
        [FunctionName("CleanupFunction")]
        public async Task<IActionResult> RunCleanupFunction(
            [HttpTrigger(AuthorizationLevel.Function, methods: "post")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            Dictionary<string,object> data = JsonConvert.DeserializeObject<Dictionary<string,Object>>(requestBody);

            if(!data.ContainsKey("ConnectionString"))
            {
                return new BadRequestObjectResult("Body containing ConnectionString for the storage account is required");
            }

            // Get the connection string from the body or the query
            string storageConnectionString = (string) data["ConnectionString"]; // value from AzureWebJobsStorage definition

            if (String.IsNullOrWhiteSpace(storageConnectionString))
            {
                return new BadRequestObjectResult("The ConnectionString cannot be empty");
            }

            var storageServiceSettings = new AzureStorageOrchestrationServiceSettings
            {
                StorageConnectionString = storageConnectionString,
                TaskHubName = client.TaskHubName,
            };

            // AzureStorageOrchestrationService is defined in Microsoft.Azure.DurableTask.AzureStorage, which
            // is an implicit dependency of the Durable Functions extension.
            var storageService = new AzureStorageOrchestrationService(storageServiceSettings);

            // This will delete all Azure Storage resources associated with this task hub
            log.LogInformation("Deleting all storage resources for task hub {taskHub}...", storageServiceSettings.TaskHubName);
            await storageService.DeleteAsync();

            // Wait for a minute since Azure Storage won't let us immediately recreate resources
            // with the same names as before.
            log.LogInformation("The delete operation completed. Giving Azure Storage a minute to internally process the deletes...");
            await Task.Delay(TimeSpan.FromMinutes(1));

            // Optional: Recreate all the Azure Storage resources for this task hub. This is done
            // automatically whenever the function app restarts, so it's not a required step.
            log.LogInformation("Recreating storage resources for task hub {taskHub}...", storageServiceSettings.TaskHubName);
            await storageService.CreateIfNotExistsAsync();

            return new OkObjectResult($"Deleted {client.TaskHubName}");
        }
    }
}
