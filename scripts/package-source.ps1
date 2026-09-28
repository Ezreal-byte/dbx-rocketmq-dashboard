$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem
$root = (Get-Location).Path
$version = (Get-Content -LiteralPath 'manifest.json' -Raw -Encoding UTF8 | ConvertFrom-Json).version
$output = Join-Path $root "dist/dbx-rocketmq-dashboard-$version-source.zip"
$files = @(& git ls-files --cached --others --exclude-standard | Sort-Object -Unique)
if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate project source' }
if (Test-Path -LiteralPath $output) { Remove-Item -LiteralPath $output }
$zip = [IO.Compression.ZipFile]::Open($output, [IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($relative in $files) {
        $absolute = [IO.Path]::GetFullPath((Join-Path $root $relative))
        if (-not $absolute.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Source path outside project' }
        [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $absolute, ('dbx-rocketmq-dashboard/' + $relative), [IO.Compression.CompressionLevel]::Optimal) | Out-Null
    }
} finally { $zip.Dispose() }
Get-FileHash -LiteralPath $output -Algorithm SHA256
