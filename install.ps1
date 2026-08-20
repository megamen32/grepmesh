$ErrorActionPreference = 'Stop'

$repo = if ($env:GREPMESH_REPO) { $env:GREPMESH_REPO } else { 'megamen32/grepmesh' }
$version = if ($env:GREPMESH_VERSION) { $env:GREPMESH_VERSION } else { 'latest' }
$prefix = if ($env:GREPMESH_PREFIX) { $env:GREPMESH_PREFIX } else { Join-Path $env:LOCALAPPDATA 'GrepMesh' }
$configDir = if ($env:GREPMESH_CONFIG_DIR) { $env:GREPMESH_CONFIG_DIR } else { Join-Path $env:APPDATA 'GrepMesh' }
$asset = 'grepmesh-windows-x86_64.zip'

if (-not [Environment]::Is64BitOperatingSystem) {
    throw 'GrepMesh binary releases currently support Windows x86_64.'
}

$url = if ($version -eq 'latest') {
    "https://github.com/$repo/releases/latest/download/$asset"
} else {
    "https://github.com/$repo/releases/download/$version/$asset"
}

$temp = Join-Path ([System.IO.Path]::GetTempPath()) ("grepmesh-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $temp | Out-Null
try {
    $archive = Join-Path $temp $asset
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $archive
    Expand-Archive -LiteralPath $archive -DestinationPath $temp -Force

    New-Item -ItemType Directory -Force -Path $prefix, $configDir | Out-Null
    Copy-Item -LiteralPath (Join-Path $temp 'grepmesh-mcp.exe') -Destination (Join-Path $prefix 'grepmesh-mcp.exe') -Force
    $bundledRg = Join-Path $temp 'rg.exe'
    if (Test-Path -LiteralPath $bundledRg) {
        Copy-Item -LiteralPath $bundledRg -Destination (Join-Path $prefix 'rg.exe') -Force
    } elseif (Get-Command rg.exe -ErrorAction SilentlyContinue) {
        Write-Host 'Using existing rg.exe from PATH.'
    } elseif (Get-Command winget.exe -ErrorAction SilentlyContinue) {
        & winget.exe install --id BurntSushi.ripgrep.MSVC --exact --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -ne 0) { throw 'Failed to install ripgrep with winget.' }
    } elseif (Get-Command choco.exe -ErrorAction SilentlyContinue) {
        & choco.exe install ripgrep -y
        if ($LASTEXITCODE -ne 0) { throw 'Failed to install ripgrep with Chocolatey.' }
    } else {
        throw 'ripgrep (rg.exe) is required for path search and is unavailable. Install ripgrep, then rerun this installer. GrepMesh text search can use its explicit grep fallback when rg is absent.'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $prefix 'rg.exe')) -and -not (Get-Command rg.exe -ErrorAction SilentlyContinue)) {
        throw 'ripgrep installation did not provide rg.exe. GrepMesh text search can use its explicit grep fallback, but this installer cannot complete without rg.exe.'
    }

    $configPath = Join-Path $configDir 'config.json'
    if (-not (Test-Path -LiteralPath $configPath)) {
        $config = Get-Content -Raw -LiteralPath (Join-Path $temp 'config.example.json') | ConvertFrom-Json
        $config.root = $HOME
        $config.roots.projects = @($HOME)
        $configJson = $config | ConvertTo-Json -Depth 8
        [System.IO.File]::WriteAllText(
            $configPath,
            $configJson + [Environment]::NewLine,
            (New-Object System.Text.UTF8Encoding($false))
        )
    }

    $binary = Join-Path $prefix 'grepmesh-mcp.exe'
    $taskName = 'GrepMesh MCP'
    $taskCommand = '"{0}" --config "{1}"' -f $binary, $configPath
    $task = Start-Process -Wait -PassThru -WindowStyle Hidden -FilePath schtasks.exe -ArgumentList @(
        '/Create', '/TN', $taskName, '/SC', 'ONLOGON', '/TR', $taskCommand, '/F'
    )
    if ($task.ExitCode -eq 0) {
        & schtasks.exe /Run /TN $taskName | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Failed to start the GrepMesh logon task.' }
    } else {
        $startup = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup'
        New-Item -ItemType Directory -Force -Path $startup | Out-Null
        [System.IO.File]::WriteAllText(
            (Join-Path $startup 'grepmesh.cmd'),
            "@start `"`" $taskCommand" + [Environment]::NewLine,
            [System.Text.Encoding]::ASCII
        )
        Start-Process -WindowStyle Hidden -FilePath $binary -ArgumentList @('--config', $configPath)
    }
} finally {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $temp
}

Write-Host 'GrepMesh installed and started at http://127.0.0.1:9419/mcp'
