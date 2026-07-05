$ErrorActionPreference = 'Stop'

$root = Resolve-Path (Join-Path $PSScriptRoot '..')
$pgBin = 'C:\Program Files\PostgreSQL\17\bin'
$pgCtl = Join-Path $pgBin 'pg_ctl.exe'
$psql = Join-Path $pgBin 'psql.exe'
$createdb = Join-Path $pgBin 'createdb.exe'
$pgData = Join-Path $root '.runtime\pgdata'
$pgLog = Join-Path $root '.runtime\postgres.log'
$databaseUrl = 'postgresql+asyncpg://postgres@127.0.0.1:55432/meteorite_db'

if (!(Test-Path $pgCtl) -or !(Test-Path $psql) -or !(Test-Path $createdb)) {
  throw "PostgreSQL 17 tools not found in $pgBin"
}

if (!(Test-Path $pgData)) {
  throw "Postgres data directory not found: $pgData"
}

$pgStatus = & $pgCtl status -D $pgData 2>&1
if ($LASTEXITCODE -ne 0) {
  & $pgCtl start -D $pgData -o '"-p" "55432" "-h" "127.0.0.1"' -l $pgLog
}

$ready = $false
for ($i = 0; $i -lt 20; $i++) {
  & $psql -h 127.0.0.1 -p 55432 -U postgres -d postgres -c 'select 1' *> $null
  if ($LASTEXITCODE -eq 0) {
    $ready = $true
    break
  }
  Start-Sleep -Milliseconds 500
}

if (!$ready) {
  throw 'Postgres did not become ready on 127.0.0.1:55432'
}

$dbExists = & $psql -h 127.0.0.1 -p 55432 -U postgres -d postgres -tAc "select 1 from pg_database where datname='meteorite_db'"
if (($dbExists -join '').Trim() -ne '1') {
  & $createdb -h 127.0.0.1 -p 55432 -U postgres meteorite_db
}

$env:TINSSIT_SKIP_MODEL_LOAD = '0'
$env:API_KEY = '5599d202610ca6d6c92480e3e6537e80cb87383af5a52260c2a71741ce289d06'
$env:DATABASE_URL = $databaseUrl
$env:STORAGE_DIR = Join-Path $root '.runtime\storage_vessel'
$env:CORS_ALLOWED_ORIGINS = '*'
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

Set-Location $root
python scripts\seed-local-user.py
Write-Output 'Tissint backend starting at http://127.0.0.1:8000'
Write-Output "DATABASE_URL=$databaseUrl"
Write-Output 'TINSSIT_SKIP_MODEL_LOAD=0 (AI scan enabled; model loading can take a moment)'
python -m uvicorn main:app --host 127.0.0.1 --port 8000
