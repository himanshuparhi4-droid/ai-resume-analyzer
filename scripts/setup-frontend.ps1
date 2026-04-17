param()

$node = Get-Command node -ErrorAction SilentlyContinue
if (-not $node) {
  Write-Host "Node.js not found. Install Node.js 20 LTS first." -ForegroundColor Red
  exit 1
}

Set-Location "$PSScriptRoot\..\frontend"
npm install
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
Write-Host "Frontend setup completed. Run: npm run dev" -ForegroundColor Green
