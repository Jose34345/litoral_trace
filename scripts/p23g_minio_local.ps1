param(
    [ValidateSet("init", "start", "status", "stop")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EnvFile = Join-Path $RepoRoot ".env.minio.integration"

$ContainerName = "litoral-trace-p23g-minio"
$ContainerImage = "quay.io/minio/minio"

$NativeDir = Join-Path $RepoRoot "tmp_p23g_minio"
$NativeExe = Join-Path $NativeDir "minio.exe"
$NativeChecksum = Join-Path $NativeDir "minio.exe.sha256sum"
$NativeDataDir = Join-Path $NativeDir "data"
$NativeStdout = Join-Path $NativeDir "minio.stdout.log"
$NativeStderr = Join-Path $NativeDir "minio.stderr.log"
$NativePidFile = Join-Path $NativeDir "minio.pid"

$NativeDownloadUrl = "https://dl.min.io/server/minio/release/windows-amd64/minio.exe"
$NativeChecksumUrl = "https://dl.min.io/server/minio/release/windows-amd64/minio.exe.sha256sum"

function Read-DotEnv {
    param([string]$Path)

    $values = @{}
    if (-not (Test-Path $Path)) {
        return $values
    }

    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }

        $parts = $line.Split("=", 2)
        $values[$parts[0].Trim()] = $parts[1].Trim()
    }

    return $values
}

function New-RandomSecret {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()

    try {
        $rng.GetBytes($bytes)
    }
    finally {
        $rng.Dispose()
    }

    $secret = [Convert]::ToBase64String($bytes)
    $secret = $secret.Replace("/", "A").Replace("+", "B").TrimEnd("=")
    return $secret
}

function Ensure-EnvFile {
    if (Test-Path $EnvFile) {
        return
    }

    $secret = New-RandomSecret

    $envContent = @"
ENABLE_MINIO_TESTS=1
TEST_MINIO_ENDPOINT_URL=http://127.0.0.1:9000
TEST_MINIO_ACCESS_KEY_ID=p23g-local-root
TEST_MINIO_SECRET_ACCESS_KEY=$secret
TEST_MINIO_BUCKET_NAME=litoral-trace-p23g
TEST_MINIO_REGION=us-east-1
"@

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText(
        $EnvFile,
        $envContent,
        $utf8NoBom
    )

    Write-Host "Created ignored local integration configuration: .env.minio.integration"
    Write-Host "Credentials were generated locally and were not printed."
}

function Get-ContainerRuntime {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        & docker info *> $null
        if ($LASTEXITCODE -eq 0) {
            return "docker"
        }
    }

    if (Get-Command podman -ErrorAction SilentlyContinue) {
        & podman info *> $null
        if ($LASTEXITCODE -eq 0) {
            return "podman"
        }
    }

    return $null
}

function Wait-MinIO {
    $healthUrl = "http://127.0.0.1:9000/minio/health/live"

    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            $response = Invoke-WebRequest `
                -Uri $healthUrl `
                -Method Head `
                -UseBasicParsing `
                -TimeoutSec 2

            if ($response.StatusCode -eq 200) {
                Write-Host "MinIO health: OK"
                return
            }
        }
        catch {
            Start-Sleep -Milliseconds 750
        }
    }

    throw "MinIO did not become healthy on loopback port 9000."
}

function Ensure-NativeMinIO {
    New-Item -ItemType Directory -Force -Path $NativeDir *> $null
    New-Item -ItemType Directory -Force -Path $NativeDataDir *> $null

    if (-not (Test-Path $NativeExe)) {
        Write-Host "No Docker/Podman runtime available. Downloading official MinIO Windows binary..."

        Invoke-WebRequest `
            -Uri $NativeDownloadUrl `
            -OutFile $NativeExe `
            -UseBasicParsing

        Invoke-WebRequest `
            -Uri $NativeChecksumUrl `
            -OutFile $NativeChecksum `
            -UseBasicParsing
    }
    elseif (-not (Test-Path $NativeChecksum)) {
        Invoke-WebRequest `
            -Uri $NativeChecksumUrl `
            -OutFile $NativeChecksum `
            -UseBasicParsing
    }

    $checksumLine = (
        Get-Content $NativeChecksum -Raw
    ).Trim()

    if (-not $checksumLine) {
        throw "MinIO checksum file is empty."
    }

    $expectedHash = (
        $checksumLine -split "\s+"
    )[0].Trim().ToLowerInvariant()

    if ($expectedHash -notmatch "^[0-9a-f]{64}$") {
        throw "MinIO checksum file has an invalid SHA-256 value."
    }

    $actualHash = (
        Get-FileHash `
            -Path $NativeExe `
            -Algorithm SHA256
    ).Hash.ToLowerInvariant()

    if ($actualHash -ne $expectedHash) {
        Remove-Item $NativeExe -Force -ErrorAction SilentlyContinue
        throw "MinIO binary SHA-256 verification failed."
    }

    Write-Host "Official MinIO binary SHA-256: verified."
}

function Get-NativeMinIOProcess {
    if (-not (Test-Path $NativePidFile)) {
        return $null
    }

    $rawPid = (
        Get-Content $NativePidFile -Raw
    ).Trim()

    $parsedPid = 0
    if (-not [int]::TryParse($rawPid, [ref]$parsedPid)) {
        Remove-Item $NativePidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    try {
        $process = Get-Process -Id $parsedPid -ErrorAction Stop
    }
    catch {
        Remove-Item $NativePidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    if ($process.ProcessName -ne "minio") {
        Remove-Item $NativePidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    return $process
}

function Start-NativeMinIO {
    param([hashtable]$Config)

    Ensure-NativeMinIO

    $existing = Get-NativeMinIOProcess
    if ($null -ne $existing) {
        Write-Host "Native MinIO process is already running."
        Wait-MinIO
        return
    }

    Remove-Item $NativeStdout -Force -ErrorAction SilentlyContinue
    Remove-Item $NativeStderr -Force -ErrorAction SilentlyContinue

    $previousRootUser = $env:MINIO_ROOT_USER
    $previousRootPassword = $env:MINIO_ROOT_PASSWORD
    $previousBrowser = $env:MINIO_BROWSER

    $env:MINIO_ROOT_USER = $Config["TEST_MINIO_ACCESS_KEY_ID"]
    $env:MINIO_ROOT_PASSWORD = $Config["TEST_MINIO_SECRET_ACCESS_KEY"]
    $env:MINIO_BROWSER = "off"

    try {
        $arguments = @(
            "server",
            "`"$NativeDataDir`"",
            "--address",
            "127.0.0.1:9000",
            "--console-address",
            "127.0.0.1:9001"
        )

        $process = Start-Process `
            -FilePath $NativeExe `
            -ArgumentList $arguments `
            -WorkingDirectory $NativeDir `
            -RedirectStandardOutput $NativeStdout `
            -RedirectStandardError $NativeStderr `
            -WindowStyle Hidden `
            -PassThru

        Set-Content `
            -Path $NativePidFile `
            -Value $process.Id `
            -Encoding ASCII
    }
    finally {
        if ($null -eq $previousRootUser) {
            Remove-Item Env:MINIO_ROOT_USER -ErrorAction SilentlyContinue
        }
        else {
            $env:MINIO_ROOT_USER = $previousRootUser
        }

        if ($null -eq $previousRootPassword) {
            Remove-Item Env:MINIO_ROOT_PASSWORD -ErrorAction SilentlyContinue
        }
        else {
            $env:MINIO_ROOT_PASSWORD = $previousRootPassword
        }

        if ($null -eq $previousBrowser) {
            Remove-Item Env:MINIO_BROWSER -ErrorAction SilentlyContinue
        }
        else {
            $env:MINIO_BROWSER = $previousBrowser
        }
    }

    Start-Sleep -Milliseconds 500

    $started = Get-NativeMinIOProcess
    if ($null -eq $started) {
        $detail = ""
        if (Test-Path $NativeStderr) {
            $detail = (
                Get-Content $NativeStderr -Tail 10
            ) -join [Environment]::NewLine
        }

        if ($detail) {
            throw "Native MinIO exited during startup.`n$detail"
        }

        throw "Native MinIO exited during startup."
    }

    Wait-MinIO
    Write-Host "P2.3G MinIO started as a native Windows process on loopback only."
}

function Stop-NativeMinIO {
    $process = Get-NativeMinIOProcess

    if ($null -eq $process) {
        Write-Host "Native P2.3G MinIO is not running."
        return
    }

    Stop-Process `
        -Id $process.Id `
        -Force

    $process.WaitForExit()
    Remove-Item $NativePidFile -Force -ErrorAction SilentlyContinue

    Write-Host "Native P2.3G MinIO stopped."
}

function Start-ContainerMinIO {
    param(
        [string]$Runtime,
        [hashtable]$Config
    )

    & $Runtime rm -f $ContainerName *> $null

    & $Runtime run `
        -d `
        --rm `
        --name $ContainerName `
        -p "127.0.0.1:9000:9000" `
        -p "127.0.0.1:9001:9001" `
        -e "MINIO_ROOT_USER=$($Config['TEST_MINIO_ACCESS_KEY_ID'])" `
        -e "MINIO_ROOT_PASSWORD=$($Config['TEST_MINIO_SECRET_ACCESS_KEY'])" `
        -e "MINIO_BROWSER=off" `
        $ContainerImage `
        server /data `
        --address "0.0.0.0:9000" `
        --console-address "0.0.0.0:9001" *> $null

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start the P2.3G MinIO container."
    }

    Wait-MinIO
    Write-Host "P2.3G MinIO started in a container on loopback only."
}

function Stop-ContainerMinIO {
    param([string]$Runtime)

    & $Runtime stop $ContainerName *> $null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Containerized P2.3G MinIO stopped."
    }
}

Ensure-EnvFile
$config = Read-DotEnv $EnvFile

switch ($Action) {
    "init" {
        Write-Host "P2.3G local configuration is ready."
        Write-Host "Endpoint: $($config['TEST_MINIO_ENDPOINT_URL'])"
        Write-Host "Bucket: $($config['TEST_MINIO_BUCKET_NAME'])"
        exit 0
    }

    "start" {
        $runtime = Get-ContainerRuntime

        if ($null -ne $runtime) {
            Start-ContainerMinIO `
                -Runtime $runtime `
                -Config $config
        }
        else {
            Start-NativeMinIO `
                -Config $config
        }

        Write-Host "Endpoint: $($config['TEST_MINIO_ENDPOINT_URL'])"
        Write-Host "Bucket: $($config['TEST_MINIO_BUCKET_NAME'])"
        exit 0
    }

    "status" {
        $runtime = Get-ContainerRuntime

        if ($null -ne $runtime) {
            & $runtime ps `
                --filter "name=$ContainerName" `
                --format "{{.Names}} {{.Status}}"
        }

        $nativeProcess = Get-NativeMinIOProcess
        if ($null -ne $nativeProcess) {
            Write-Host "Native MinIO PID: $($nativeProcess.Id)"
        }

        try {
            Wait-MinIO
        }
        catch {
            Write-Host "MinIO health: NOT READY"
            exit 1
        }

        exit 0
    }

    "stop" {
        $runtime = Get-ContainerRuntime

        if ($null -ne $runtime) {
            Stop-ContainerMinIO `
                -Runtime $runtime
        }

        Stop-NativeMinIO
        exit 0
    }
}