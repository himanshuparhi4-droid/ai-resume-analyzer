param()

Write-Host "Checking local machine for AI Resume Analyzer..." -ForegroundColor Cyan

$pythonVersions = py -0p 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Python launcher not found. Install Python 3.11 or 3.12 first." -ForegroundColor Red
} else {
  Write-Host "Installed Python versions:" -ForegroundColor Green
  Write-Host $pythonVersions
  if ($pythonVersions -notmatch '3\.11' -and $pythonVersions -notmatch '3\.12') {
    Write-Host "Recommended: install Python 3.11 or 3.12. Python 3.14 is too new for parts of this ML stack." -ForegroundColor Yellow
  }
}

$node = Get-Command node -ErrorAction SilentlyContinue
if (-not $node) {
  Write-Host "Node.js is not installed. Install Node.js 20 LTS." -ForegroundColor Yellow
} else {
  Write-Host "Node.js found: $((node --version))" -ForegroundColor Green
}

$tesseract = Get-Command tesseract -ErrorAction SilentlyContinue
if (-not $tesseract) {
  Write-Host "Tesseract not found. OCR features will stay disabled until you install it." -ForegroundColor Yellow
} else {
  Write-Host "Tesseract found." -ForegroundColor Green
}
