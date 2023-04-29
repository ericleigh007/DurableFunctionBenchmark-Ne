function invoke-benchmark {
    [CmdletBinding()]
    param (
     [string]$uri,
     [string]$key,
     [switch]$Direct,
     [switch]$MixedPartitionKey,
     [switch]$Bulk,
     [int]$Orchestrators,
     [int]$Activities,
     [int]$items,
     [int]$documentSize
    )
    
    $directsw = $false
    $directString = "[Indirectly launched]"
    if($direct)
    {
        $directSw = $true
        $directString = "[Directly launched]"
    }

    $useMixedParts = $false
    $partString = "[Runid Partition Key]"
    if($MixedPartitionKey)
    {
        $useMixedParts = $true
        $partString = "[Mixed Partition Key]"
    }

    $useBulk = $false
    $bulkString = "[Normal Cosmos Container]"
    if($bulk)
    {
        $useBulk = $true
        $bulkString = "[Bulk Cosmos Container]"
    }

    $body = @{ "SubOrchestratorCount" = $Orchestrators;
               "ActivityCount" = $Activities;
               "ItemCount" = $items;
               "Direct" = $DirectSw
               "UseMixedPartitionKey" = $useMixedParts;
               "UseBulk" = $useBulk;
               "DocumentSize" = $documentSize }

    $jsonBody = $body | convertto-json

    # the base URI is the administrative one, not the trigger URI. This is necessary to get the 
    # 202 behavior
    if(-not [string]::IsNullOrWhiteSpace($key))
    {
        $builtUri = ($Uri + "?code=" + $key)
    }
    
    $response = Invoke-WebRequest -Method Post -uri $builtUri -body $jsonBody -ContentType "application/json"
    if($response.StatusCode -eq 200)
    {
        # we're done
        return $response.Content | convertfrom-json | select Status
    }

#    $theStatus = $null
#    $customStatus = $null
#    $functionStatus = $null
#    $runtimeStatus = $null

    if( -not ($response.StatusCode -in (200,202)))
    {
        write-error "error obtaining status -- called failed"
        return
    }

    # get the status URI from the response
    $statusUri = ($response | convertfrom-json | select statusQueryGetUri).statusQueryGetUri
    $terminateUri = ($response | convertfrom-json | select terminatePostUri).terminatePostUri

    $tries = 0
    $startTime = [DateTime]::UtcNow
    write-host "[$startTime] Waiting for $directString $partString $bulkString function to complete ... querying the status ..."

    while($true)
    {
        write-verbose "Checking status from status Uri ..."
        $response = Invoke-WebRequest -Method get -Uri $statusUri
        if($response.StatusCode -in (200,202))
        {
            Write-verbose "Response is $($response.StatusCode)"
            $functionStatus = ($response.content | convertfrom-json)
            $runtimeStatus = $functionStatus.runtimeStatus
            write-verbose "function status is $functionStatus"
            if($runtimeStatus -eq "Completed")
            {
                $customStatus = $functionStatus.CustomStatus
                $expected = $customStatus.ExpectedReturnCount
                $obtained = $customStatus.ReturnCount
                $lastCharge = $customStatus.LastQueryCharge
                $lastTime = $customStatus.LastQueryTime
                $ItemsPerSecond = $customStatus.ItemsPerSecond

                if($documentSize -eq 0)
                {
                    write-host "Function is completed, got $obtained of $expected outputs ($itemsPerSecond) items/sec"
                }
                else 
                {
                    write-host "Function is completed, got $obtained of $expected documents ($itemsPerSecond items/sec), query $lastCharge RU in $lastTime seconds"
                }
                break;
            }
            elseif(-not ($runtimeStatus -in ("Running","Pending")))
            {
                write-error "function maybe not running, function status $runtimeStatus"
                if($runtimeErrorCount -gt 5)
                {
                    break
                }
                start-sleep -seconds 2
            }

            $customStatus = $functionStatus.customStatus
            if($customStatus -eq $null)
            {
                write-verbose "Error obtaining status, still null"
                continue
            }

            $expected = $customStatus.ExpectedReturnCount
            $obtained = $customStatus.ReturnCount
            $lastCharge = $customStatus.LastQueryCharge
            $lastTime = $customStatus.LastQueryTime

            if($expected -eq $obtained)
            {
                if($customStatus.Status -eq "Complete")
                {
                    if($documentSize -gt 0 -and $customStatus.StatisticsDocument -ne $null)
                    {
                        write-host "function is complete and stats document is present"
                        break
                    }
                    elseif( $documentSize -eq 0)
                    {
                        write-host "function measuring output throughput only is complete"
                        break
                    }
                }
            }

            $customStatus
            
            write-verbose $customStatus.Message
            write-verbose $customStatus.Status

            $interval = [DateTime]::UtcNow - $startTime

            if($documentSize -gt 0)
            {
                if($expected)
                {                
                    Write-host "In progress($tries) [$interval], got $obtained out of $expected documents with $lastCharge RUs in $lastTime seconds (press a key to exit)"
                }
                else
                {
                    write-host "launching($tries) [$interval] $orchestrators orchestrators, each with $activities, each with $items items ($($orchestrators*$activities*$items) items) (press a key to exit)..."
                }
            }
            else 
            {
                Write-host "In progress($tries) [$interval], got $obtained out of $expected outputs (press a key to exit)"                
            }

            if([console]::KeyAvailable)
            {
                write-host "Key interupted processing.  Terminate?"
                $ans = read-host "Enter [Y]es to terminate"
                if($ans -eq "Y")
                {
                    invoke-webrequest -uri $terminateUri -Method post

                    return $null
                }
            }
            $tries++
        }

        $retryTime = $response.headers["Retry-After"]
        start-sleep -Seconds $retryTime
    }

    if($documentSize -eq 0)
    {
        return $customStatus
    }

    return $customStatus.StatisticsDocument
}

# $resetjson =  "DefaultEndpointsProtocol=https;AccountName=rguksels1devmorebench;AccountKey=a0/5fZgDSAE57xxxxxxxxxxyWcmTb6UU7Y3B8NvowU4mzBi60bvD3D36M4gDiRQxiFmz/Tc5tAc+AStOui3xQ==;EndpointSuffix=core.windows.net"
# $reseturi = "https://durablefunctionbenchmark.azurewebsites.net/api/CleanupFunction?code=yUxxxxxxxx2vKuLQ9pQvmSAL9yxhLwy3_Qx-aJ7iI0UZlAzFueRA3Cg=="



# $uri = "https://durablefunctionbenchmark-ne.azurewebsites.net/api/BandLeader?code=-pkZUf2lVMsmAhO-XRNxxxxxxxxxxxfL5-VovTA_jIOMo0W7AzFu7IjHKw=="

#$json = @"{
#    "DatabaseName":  "transactiondatabase",
#    "ConnectionString":  "AccountEndpoint=https://dba-uks-els1l-dev-transactiondatabase-perf.documents.azure.com:443/;AccountKey=K4i7BgYQXaxjn8Qe5VlLg08BtgxvckRgsd015xxxxxxxxxxxxxxxxxxxXpK9F3MR5zq2d6q7ACDb604XHg==",
#    "ContainerName":  "Transactions"
#}"

$newBody = new-object PSObject @{  
    "SubOrchestratorCount" = 100;
    "ActivityCount" = 10;
    "ItemCount" = 1;
    "Direct" = $true;
    "UseMixedPartitionKey" = $true;
    "UseBulk" = $false;
}

$newjsonbody = $newBody | convertto-json

$localTestUri = "http://localhost:7180/api/BandLeader"
$localCleanupUri = "http://localhost:7180/api/CleanupFunction"

$bandleaderKey = "-pkZUf2lVMsmAhO-XRNPim9kNKfL5-VovTA_jIOMo0W7AzFu7IjHKw=="
$uri = "https://durablefunctionbenchmark-ne.azurewebsites.net/api/BandLeader"
$uriWithKey = "https://durablefunctionbenchmark-ne.azurewebsites.net/api/BandLeader?code=-pkZUf2lVMsmAhO-XRxxxxxxxxxxxxxxxxxxxxxxxxxOMo0W7AzFu7IjHKw=="

#CreateAndInitialize
#93c2c485-f325-4e37-93e1-5b383b3d6090

#new CosmosClient
#ba7db60d-26c1-4cce-8191-756fb79b7886

# reset
#Invoke-WebRequest -uri $resetUri -body $resetJson -Method post

#run 
#((invoke-webrequest -uri "https://durablefunctionbenchmark.azurewebsites.net/api/BandLeader?code=HCot5kPjIPTgH0m9J2ZpZm2y1wWwtcT0ogNFzcjDUd1OAzFu2XKSQw==&Orchestrators=5&Activities=10" -body $json -Method Post).Content | convertfrom-json).Status

