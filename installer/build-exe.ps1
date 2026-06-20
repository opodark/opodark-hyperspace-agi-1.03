# build-exe.ps1 — Build locale HyperSpaceAGI-Installer.exe
# Uso: cd installer && .\build-exe.ps1
# Output: installer\dist\HyperSpaceAGI-Installer.exe  +  HyperSpaceAGI-Installer.zip

$ErrorActionPreference = "Stop"
$VERSION = "1.02"
$EXE_NAME = "HyperSpaceAGI-Installer"

# ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ⬡  HyperSpace AGI — Build Installer v$VERSION" -ForegroundColor Cyan
Write-Host "─────────────────────────────────────────────────────" -ForegroundColor DarkGray
Write-Host ""

# ── 0. Check Python ──────────────────────────────────────────────
Write-Host "[0/5] Verifica Python..." -ForegroundColor Yellow
try {
    $pyver = python --version 2>&1
    Write-Host "      ✅  $pyver" -ForegroundColor Green
} catch {
    Write-Host "      ❌  Python non trovato. Installa Python 3.9+ da https://python.org" -ForegroundColor Red
    exit 1
}

# ── 1. Dipendenze ────────────────────────────────────────────────
Write-Host ""
Write-Host "[1/5] Installazione dipendenze pip..." -ForegroundColor Yellow
pip install --quiet --upgrade customtkinter requests pyinstaller
if ($LASTEXITCODE -ne 0) {
    Write-Host "      ❌  pip install fallito." -ForegroundColor Red
    exit 1
}
Write-Host "      ✅  customtkinter, requests, pyinstaller pronti." -ForegroundColor Green

# ── 2. Pulizia build precedente ──────────────────────────────────
Write-Host ""
Write-Host "[2/5] Pulizia build precedente..." -ForegroundColor Yellow
$toRemove = @("dist", "build", "$EXE_NAME.spec")
foreach ($item in $toRemove) {
    if (Test-Path $item) {
        Remove-Item -Recurse -Force $item
        Write-Host "      🗑️  Rimosso: $item"
    }
}

# ── 3. PyInstaller ───────────────────────────────────────────────
Write-Host ""
Write-Host "[3/5] Build .exe con PyInstaller..." -ForegroundColor Yellow
Write-Host "      (può richiedere 1-2 minuti)" -ForegroundColor DarkGray

# Cerca icona opzionale
$iconFlag = ""
if (Test-Path "icon.ico") {
    $iconFlag = "--icon=icon.ico"
    Write-Host "      🎨  Icona trovata: icon.ico"
}

# Controlla se i file aggiuntivi esistono
$addDataFlags = ""
if (Test-Path "..\docker-compose.yml") {
    $addDataFlags += " --add-data `"..\docker-compose.yml;.`""
}
if (Test-Path "..\.env.example") {
    $addDataFlags += " --add-data `"..\.env.example;.`""
}

$cmd = "pyinstaller --onefile --windowed --name `"$EXE_NAME`" $iconFlag $addDataFlags hyperspace-installer.pyw"
Write-Host "      > $cmd" -ForegroundColor DarkGray
Invoke-Expression $cmd

if ($LASTEXITCODE -ne 0) {
    Write-Host "      ❌  PyInstaller fallito. Controlla l'output sopra." -ForegroundColor Red
    exit 1
}

$exePath = "dist\$EXE_NAME.exe"
if (-not (Test-Path $exePath)) {
    Write-Host "      ❌  .exe non trovato in $exePath" -ForegroundColor Red
    exit 1
}

$sizeMB = [math]::Round((Get-Item $exePath).Length / 1MB, 1)
Write-Host "      ✅  $exePath  ($sizeMB MB)" -ForegroundColor Green

# ── 4. Crea zip per GitHub Release ──────────────────────────────
Write-Host ""
Write-Host "[4/5] Creazione zip per GitHub Release..." -ForegroundColor Yellow
$zipName = "$EXE_NAME-v$VERSION-win64.zip"
$zipPath = "dist\$zipName"

# Aggiungi README se esiste
$filesToZip = @($exePath)
if (Test-Path "README.md") { $filesToZip += "README.md" }

Compress-Archive -Path $filesToZip -DestinationPath $zipPath -Force
$zipMB = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
Write-Host "      ✅  $zipPath  ($zipMB MB)" -ForegroundColor Green

# ── 5. Riepilogo + istruzioni release ────────────────────────────
Write-Host ""
Write-Host "[5/5] Build completata!" -ForegroundColor Green
Write-Host ""
Write-Host "─────────────────────────────────────────────────────" -ForegroundColor DarkGray
Write-Host "  📦  File pronti:" -ForegroundColor White
Write-Host "      .exe  →  $((Resolve-Path $exePath).Path)" -ForegroundColor Cyan
Write-Host "      .zip  →  $((Resolve-Path $zipPath).Path)" -ForegroundColor Cyan
Write-Host ""
Write-Host "  🚀  Per pubblicare su GitHub Releases:" -ForegroundColor White
Write-Host "      1. Vai su https://github.com/opodark/hyperspace-agi-1.02/releases/new"
Write-Host "      2. Tag: v$VERSION  |  Title: HyperSpace AGI v$VERSION"
Write-Host "      3. Trascina il file .zip nella sezione 'Assets'"
Write-Host "      4. Pubblica release"
Write-Host ""
Write-Host "  ✅  Testa prima l'exe: dist\$EXE_NAME.exe" -ForegroundColor Yellow
Write-Host "─────────────────────────────────────────────────────" -ForegroundColor DarkGray
Write-Host ""
