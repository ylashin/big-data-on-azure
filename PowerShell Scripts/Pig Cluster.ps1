# the below can be used with sevice principals to allow for un-attended execution
Login-AzureRmAccount 

# If you have multiple subscriptions, set the one to use
# $subscriptionID = "<subscription ID to use>"
# Select-AzureRmSubscription -SubscriptionId $subscriptionID

# Get user input/default values
$resourceGroupName = "PigGroup"
$location = "West US 2"

# Create the resource group
New-AzureRmResourceGroup -Name $resourceGroupName -Location $location

# Pick a unique name or prefix/suffix it with timestamp/etc
$defaultStorageAccountName = "pigstorage20180415"

# Create an Azure storae account and container
New-AzureRmStorageAccount `
    -ResourceGroupName $resourceGroupName `
    -Name $defaultStorageAccountName `
    -Type Standard_LRS `
    -Location $location

$defaultStorageAccountKey = (Get-AzureRmStorageAccountKey `
                                -ResourceGroupName $resourceGroupName `
                                -Name $defaultStorageAccountName)[0].Value

$defaultStorageContext = New-AzureStorageContext `
                                -StorageAccountName $defaultStorageAccountName `
                                -StorageAccountKey $defaultStorageAccountKey

# Get information for the HDInsight cluster
$clusterName = "pigforgdelt"
$clusterPassword = ConvertTo-SecureString "BigData2018" -AsPlainText –Force # use a strong password :)
# Cluster login is used to secure HTTPS services hosted on the cluster
$httpCredential = New-Object -TypeName pscredential –ArgumentList "admin", $clusterPassword
# SSH user is used to remotely connect to the cluster using SSH clients
$sshCredentials = New-Object -TypeName pscredential –ArgumentList "sshadmin", $clusterPassword

# Default cluster size (# of worker nodes), version, type, and OS
$clusterSizeInNodes = "4"
$clusterVersion = "3.6"
$clusterType = "HADOOP"
$clusterOS = "Linux"


# Create a blob container. This holds the default data store for the cluster.
New-AzureStorageContainer  -Name "hdfs-home" -Context $defaultStorageContext

$additionalStorageAccountName = "YOUR-GDELT-STORAGE-ACCOUNT-NAME"
$additionalStorageAccountKey  = "YOUR-GDELT-STORAGE-ACCOUNT-KEY"
$config = New-AzureRmHDInsightClusterConfig
Add-AzureRmHDInsightStorage -Config $config -StorageAccountName "$additionalStorageAccountName.blob.core.windows.net" -StorageAccountKey $additionalStorageAccountKey

$startTime = Get-Date

# Create the HDInsight cluster
New-AzureRmHDInsightCluster `
    -ResourceGroupName $resourceGroupName `
    -ClusterName $clusterName `
    -Location $location `
    -ClusterSizeInNodes $clusterSizeInNodes `
    -ClusterType $clusterType `
    -OSType $clusterOS `
    -Version $clusterVersion `
    -HttpCredential $httpCredential `
    -DefaultStorageAccountName "$defaultStorageAccountName.blob.core.windows.net" `
    -DefaultStorageAccountKey $defaultStorageAccountKey `
    -DefaultStorageContainer "hdfs-home" `
    -SshCredential $sshCredentials `
    -Config $config `
    -WorkerNodeSize "Standard_D13_V2"

[console]::beep(2000,500)
$endTime = Get-Date
$delta = $endTime.Subtract($startTime).TotalMinutes
Write-Host "Operation completed in $delta minutes"

# Remove-AzureRmResourceGroup  -Name $resourceGroupName -Force