function invoke-benchmarkLoop
{
    [CmdletBinding()]

    Param(
        [int]$loops,
        [string]$uri,
        [string]$key,
        [int]$orchestrators,
        [int]$activities,
        [int]$items,
        [switch]$direct,
        [switch]$mixedPart,
        [switch]$bulk,
        [int]$documentSize
    )

    $guidList = @()
    $statList = @()

    $directSw = "-Direct"
    if(-not $direct.IsPresent)
    {
        $directsw = ""
    }

    $maxProc = 0

    for( $l = 1; $l -le $loops; $l++)
    {
        write-warning "Beginning loop $l of $loops..."
        # time to break out here

        start-sleep -Seconds 1

        # figure out the hashtable way to pass these switches
        $tmpStats = invoke-benchmark -uri $uri -key $key -Orchestrators $orchestrators -Activities $activities -Items $items -MixedPartitionKey  -DocumentSize $documentSize -Direct

        if($tmpStats -eq $null)
        {
            # user pressed a key to stop the test
            return    
        }

        [int]$ItemsPerSecond = $tmpStats.ProcessedItemsPerSecond
        [int]$lItemsPerSecond = $tmpStats.LaunchedItemsPerSecond

        if($ItemsPerSecond -eq 0)
        {
            $ItemsPerSecond = $tmpStats.ItemsPerSecond
        }

        if($ItemsPerSecond -gt $maxProc)
        {
            $maxProc = $ItemsPerSecond
        }

        $runId = $tmpStats.RunId

        write-warning "Loop $l (run $runId): $ItemsPerSecond Items per second - $lItemsPerSecond Launching speed"

        $statList += $tmpStats
        $guidList += $runId
    }

    write-host "Test completed after $loops loops -- max processed items per second $maxProc"

    return $statList
}
