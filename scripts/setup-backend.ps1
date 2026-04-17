param(
  [string]$PythonVersion = "3.12"
)

$pythonPath = (py -0p | Select-String $PythonVersion | Select-Object -First 1).ToString().Split()[-1]
if (-not $pythonPath) {
  Write-Host "Python $PythonVersion not found. Install Python 3.11 or 3.12 first." -ForegroundColor Red
  exit 1
}

Set-Location "$PSScriptRoot\..\backend"
& $pythonPath -m venv .venv
. .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-ml.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
Write-Host "Backend setup completed. Run: . .\.venv\Scripts\Activate.ps1 ; uvicorn app.main:app --reload" -ForegroundColor Green
