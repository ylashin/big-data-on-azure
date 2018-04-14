Install-Module PowerShellGet -Force
Set-PSRepository -Name PSGallery -InstallationPolicy Trusted
Install-Module AzureRM -AllowClobber
Install-Module Azure

$StorageAccountName = "YOUR-STORAGE-ACCOUNT-NAME" 
$StorageAccountKey = "YOUR-STORAGE-ACCOUNT-KEY"

function GetDownloadFileUrls () {
    $BaseUrl = "http://gdelt-open-data.s3.amazonaws.com/events/"

    # 1979 - 2005 : yearly
    # 2006 and March, 2013 : month and year
    # April 1, 2013, files are created daily

    $FileUrls = @()

    for ($year = 1979; $year -le 2005; $year++) {
        $FileUrls += "$BaseUrl$year.csv"
    }

    for ($year = 2006; $year -le 2013; $year++) {
        for ($month = 1; $month -le 12; $month++) {
            if ($year -eq 2013 -and $month -ge 4) {
                break;
            }
            $formattedMonth = $month.ToString("00")
            $FileUrls += "$BaseUrl$year$formattedMonth.csv"
        }
    }

    $StartDate = Get-Date "2013-04-01"

    #  get all days since 01/04/2013 till 2 days from today just to be sure files are available
    while ($StartDate -lt (Get-Date).Subtract(2)) {
        $formattedDay = $StartDate.ToString("yyyyMMdd")    
        $FileUrls += "$BaseUrl$formattedDay.export.csv"
        $StartDate = $StartDate.AddDays(1)
    }

    return $FileUrls
}

function GetAzureStorageContext() {   

    $ctx = New-AzureStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey
    return $ctx
}

function UploadFile($AzureStorageCtx, $ContainerName, $LocalFile) {
    $BlobName = [System.IO.Path]::GetFileName($LocalFile).Trim()
    Set-AzureStorageBlobContent -File $LocalFile -Container $ContainerName -Blob $BlobName `
        -Context $AzureStorageCtx -Properties @{"ContentType" = "text/csv"}
}

function DoesFileExistAtDestination($Url)
{
    try {
        Invoke-WebRequest -Uri $Url -Method Head
        return $true
    }
    catch {
        return $false
    }
}

function DownloadAllFiles($FileUrls)
{
    $FileCount = $FileUrls.Count
    Write-Host -ForegroundColor DarkGreen "Downloading $FileCount files ..."

    $ctx = GetAzureStorageContext
    New-Item -ItemType Directory -Force -Path "C:\AzureTemp"

    foreach ($Url in $FileUrls) {
        Try {            
            
            $fileName = [System.IO.Path]::GetFileName($Url)
            
            $outputFile = "C:\AzureTemp\$fileName"
            
            $fileHandled = DoesFileExistAtDestination "https://bigdisk.blob.core.windows.net/gdelt/$fileName"

            if ($fileHandled -eq $false)
            {
                Invoke-WebRequest -Uri $Url -OutFile $outputFile
            
                Write-Host -ForegroundColor DarkGreen "Downloaded $Url"

                UploadFile $ctx "gdelt" $outputFile
    
                Write-Host -ForegroundColor DarkGreen "Uploaded file $fileName"

                [System.IO.File]::Delete($outputFile)
            }else
            {
                Write-Host "File $fileName already handled .."
            }            
        }
        Catch {
            Write-Host "Failed downloading $Url" -ForegroundColor Yellow
            Write-Host $_.Exception.Message -ForegroundColor Yellow
            Write-Host "=======================================================" -ForegroundColor Yellow
        }
    }
}

$FileUrls = GetDownloadFileUrls
DownloadAllFiles $FileUrls