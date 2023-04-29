using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DurableFunctionBenchmark
{
    static class Utils
    {
        static Random jitter = new Random();

        public static int GetRetryWait(TimeSpan delay)
        {
            var cosmosDelay = (int)delay.TotalMilliseconds;
            var jitterMax = (int)(delay.TotalMilliseconds * 0.2);

            return cosmosDelay + jitter.Next(jitterMax);
        }

        public static QueryRequestOptions queryRequestOptions = new QueryRequestOptions()
        {
            PopulateIndexMetrics = true,
        };

        public static string indexMetrics;
        internal static QueryDefinition MakeQuery(string queryString)
        {
            return new QueryDefinition(queryString);
        }

        internal static async Task<dynamic> MeasureQueryV3<T>(Container container, QueryDefinition queryDefinition)
        {
            var sw = new Stopwatch();
            var docs = new List<T>();

            double totalCharge = 0;
            CosmosDiagnostics lastDiagnostics = null;

            FeedIterator<T> feedIterator = null;
            var maxItemsExceeded = false;
            var charges = new List<double>();
            var diagnosticsList = new List<CosmosDiagnostics>();

            int retriesAttempted = 0;
            TimeSpan retryTimeSpan = TimeSpan.Zero;

            feedIterator = container.GetItemQueryIterator<T>(
                        queryDefinition: queryDefinition,
                        continuationToken: null,
                        requestOptions: queryRequestOptions);

            while (feedIterator.HasMoreResults && !maxItemsExceeded)
            {
                FeedResponse<T> feedResponse = null;
                sw.Start();

                while (true)
                {
                    try
                    {
                        feedResponse = await feedIterator.ReadNextAsync();
                        break;
                    }
                    catch (CosmosException cx) when (cx.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        retriesAttempted++;
                        var retryTime = Utils.GetRetryWait(cx.RetryAfter.Value);
                        retryTimeSpan += TimeSpan.FromMilliseconds(retryTime);
                        await Task.Delay(retryTime);
                    }
                    catch (CosmosException cx)
                    {
                        _ = cx;
//                        log.LogError($"COSMOSQEXCEPTION: cosmos other exception \n {cx.Message}");
                        break;
                    }
                }

                sw.Stop();

                charges.Add(feedResponse.Headers.RequestCharge);

                totalCharge += feedResponse.Headers.RequestCharge;
                diagnosticsList.Add(feedResponse.Diagnostics);
                lastDiagnostics = feedResponse.Diagnostics;
                indexMetrics = feedResponse.IndexMetrics;

                foreach (T item in feedResponse)
                {
                    docs.Add(item);

                    // No-op check that forces any lazy logic to be executed
                    if (item == null)
                    {
                        throw new Exception("Null item was returned");
                    }
                }
            }

            string diags = String.Empty;
            string diagText = String.Empty;

            int indx = 0;
            try
            {
                foreach (var dig in diagnosticsList)
                {
                    // this moves around from time to time, but seems to always be called by this name
                    //pretty-print the jammed up JSON
                    var ld = JsonConvert.DeserializeObject<JObject>(dig.ToString());

                    diags += ld.ToString() + "\n";

                    List<JToken> oo = null;
                    try
                    {
                        oo = ld.SelectTokens("$..['Query Metrics']").ToList();
                    }
                    catch (Exception ex)
                    {
                        _ = ex;
                    }

                    // now pretty up the string
                    if (oo is not null)
                    {
                        foreach (var o in oo)
                        {
                            diagText += $"Query Metrics Part {++indx,3}: \n" + o.ToString().Replace("\r\n", "\n") + '\n';
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _ = ex;
            }

            var returnDoc = new
            {
                Documents = docs,
                RUCharge = totalCharge,
                ChargePieces = charges,
                ItemCountLimited = maxItemsExceeded,
                FullDiagnostics = diags,
                QueryMetrics = diagText,
                IndexMetrics = indexMetrics,
                ElapsedTime = sw.Elapsed,
                RetriesAttempted = retriesAttempted,
                RetryTimeSpan = retryTimeSpan,
            };

            return returnDoc;
        }
    }
}
