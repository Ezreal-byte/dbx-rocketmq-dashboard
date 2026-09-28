param([string]$Package = 'dist/io.dbx.rocketmq-dashboard-console-0.3.0-windows-x64.dbxp')
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $Package))
try {
    $entry = $archive.GetEntry('checksums.json')
    $reader = [IO.StreamReader]::new($entry.Open(), [Text.Encoding]::UTF8)
    try { $checksums = $reader.ReadToEnd() | ConvertFrom-Json } finally { $reader.Dispose() }
    $verified = 0
    foreach ($property in $checksums.files.PSObject.Properties) {
        $file = $archive.GetEntry($property.Name)
        if ($null -eq $file) { throw "Missing package entry: $($property.Name)" }
        $stream = $file.Open()
        $sha = [Security.Cryptography.SHA256]::Create()
        try { $actual = [Convert]::ToHexString($sha.ComputeHash($stream)).ToLowerInvariant() } finally { $stream.Dispose(); $sha.Dispose() }
        if ($actual -ne $property.Value) { throw "Checksum mismatch: $($property.Name)" }
        $verified++
    }
    if ($null -eq $archive.GetEntry('bin/windows-x64/dbx-rocketmq-dashboard.exe')) { throw 'Missing sidecar' }
    if ($null -eq $archive.GetEntry('ui/index.html')) { throw 'Missing iframe UI' }
    $metadata = Get-Content -LiteralPath ($Package -replace '\.dbxp$', '.artifact.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ((Get-FileHash -LiteralPath $Package -Algorithm SHA256).Hash.ToLowerInvariant() -ne $metadata.sha256) { throw 'Artifact digest mismatch' }
    "PASS: $verified package files match checksums; sidecar, UI and artifact digest verified."
} finally { $archive.Dispose() }
