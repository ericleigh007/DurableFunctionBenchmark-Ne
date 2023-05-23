# DurableFunctionBenchmark-Ne
A stresser and benchmark for Azure durable functions - default and netherite backends

In order to compare backends, we created this benchmark which is run as a function on your suitably-scaled-out backend and Cosmos dB.

Cosmos DB is used to record benchmark statistics for each activity and to create an overall statistics document for the run.

First, plublish the function to your resource group.

Set up the required environment variables:
`EventHubsConnection` for Netherite backend, not required for default backend

- `DurableBenchmark_ConnectionString` the connection string of your Cosmos DB database which is used for tracking
- `DurableBenchmark_ContainerName` the container name in Cosmos DB to use
- `DurableBenchmark_DatabaseName` the database name in Cosmos DB to use

In order to start a test, the HTTP trigger of the benchmark, called `BandLeaader` is invoked using `invoke-webrequest` or anything similar, as a POST, passing the JSON
body containing these elements:

```
$newBody = new-object PSObject @{  
    "SubOrchestratorCount" = 100;
    "ActivityCount" = 10;
    "ItemCount" = 1;
    "Direct" = $true;
    "UseMixedPartitionKey" = $true;
    "DocumentSize" = 100;
    "UseBulk" = $false;
}
```

In order to clean up the task hub, we've also included a `CleanupFunction`, whose HTTP endpoint is to be sent a POST JSON object containing:
```
{
    "ConnectionString":  "DefaultEndpointsProtocol=https;AccountName=sauksels1ldevbench;AccountKey=jdhn5kkz3fzKxxxxxxxyouthoughthiswasagoodkeYH4ojaoC9FTwL0r93WnojJIc8kiTQPzRUJIe4vj0ft+AStwGKJGg==;EndpointSuffix=core.windows.net"  
}
```

Basically, the benchmark uses the `BandLeader` HTTP trigger, to either launch directly (`Direct` mode) or indirectly, a number of 
orchestrators `SubOrchestratorCount`, and each of them a number of activities, `ActivityCount`, as fast as possible.  If `DocumentSize` is greater than 0
(and in the current version, has no other significance), then Cosmos DB documents are used to track progress, and the throughput can be constrained by Cosmos DB
scaling.   If `DocumentSize` is 0, then the benchmark just passes status back and forth using the backend.
