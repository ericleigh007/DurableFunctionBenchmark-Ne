using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace DurableFunctionBenchmark
{
    public static class TransactionsChangeTrigger
    {
        // CheckpointInterval - time in mS between samples of the feed
        // CheckpointDocumentCount - number of docs to include in the trigger
        // MaxItemsPerInvocation - maximum number of documents for each trigger
        [FunctionName("TransactionsChangeTrigger")]
        public static void Run([CosmosDBTrigger(
            databaseName: "transactionsDatabase",
            collectionName: "transactions",
            ConnectionStringSetting = "CosmosDBKey",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                var separateItems = true;

                log.LogInformation("Documents modified " + input.Count);
                log.LogInformation("First document Id " + input[0].Id);

                log.LogInformation("change feed trigger got {count} items", input.Count);

                var documents = new List<Document>();

                var tSets = documents.Where(d => d.GetPropertyValue<string>("DocumentType") == "TransactionSet").ToList();
                var tItems = documents.Where(d => d.GetPropertyValue<string>("DocumentType") == "TransactionItem").ToList();

                log.LogInformation($"change feed trigger got {tSets.Count} tSets and {tItems.Count} items");

                if (tSets.Any())
                {
                    if (separateItems)
                    {
                        foreach (var tS in tSets)
                        {
                            starter.StartNewAsync("TSetOrchestrator", new List<Document>() { tS });
                        }
                    }
                    else
                    {
                        starter.StartNewAsync("TSetOrchestrator", tSets);
                    }
                }

                if (tItems.Any())
                {
                    if (separateItems)
                    {
                        foreach (var tI in tItems)
                        {
                            starter.StartNewAsync("TItemOrchestrator", new List<Document>() { tI });
                        }
                    }
                    else
                    {
                        starter.StartNewAsync("TItemOrchestrator", tItems);
                    }
                }
            }
        }
    }
}
