Param(
    [switch]$NoChrome,
    [switch]$NoOllama
)

$ErrorActionPreference = "Stop"

Write-Host "=== Installation de l'Application Gestion Emploi ===" -ForegroundColor Cyan

# Aller dans le dossier du script
Set-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Path)

function Test-Python {
    try {
        $v = & python --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Python trouvé : $v"
            return $true
        }
    } catch {
        # ignore
    }
    return $false
}

function Ensure-Python {
    if (Test-Python) { return }

    Write-Host "Python n'est pas installé ou pas dans le PATH." -ForegroundColor Yellow

    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Host "Installation de Python 3.12 via winget..." -ForegroundColor Cyan
        winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    } else {
        Write-Warning "winget non disponible. Installe Python 3.12 depuis python.org, ajoute-le au PATH puis relance ce script."
        Read-Host "Appuie sur Entrée quand Python est installé pour continuer"
    }

    if (-not (Test-Python)) {
        throw "Python n'est toujours pas accessible. Abandon."
    }
}

function Ensure-Venv-And-Dependencies {
    Write-Host "Création / mise à jour de l'environnement virtuel (.venv)..." -ForegroundColor Cyan

    if (-not (Test-Path ".venv")) {
        python -m venv .venv
    }

    $venvPython = Join-Path ".venv" "Scripts\python.exe"

    & $venvPython -m pip install --upgrade pip

    if (Test-Path "requirements.txt") {
        Write-Host "Installation des dépendances depuis requirements.txt..." -ForegroundColor Cyan
        & $venvPython -m pip install -r requirements.txt
    } else {
        Write-Warning "requirements.txt introuvable, aucune dépendance installée."
    }
}

function Test-Chrome {
    if (Get-Command "chrome" -ErrorAction SilentlyContinue) { return $true }

    if (Test-Path "C:\Program Files\Google\Chrome\Application\chrome.exe") { return $true }
    if (Test-Path "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe") { return $true }

    return $false
}

function Ensure-Chrome {
    if ($NoChrome) {
        Write-Host "Skip installation de Chrome (option -NoChrome)." -ForegroundColor Yellow
        return
    }

    if (Test-Chrome) {
        Write-Host "Google Chrome déjà installé." -ForegroundColor Green
        return
    }

    Write-Host "Google Chrome non trouvé." -ForegroundColor Yellow
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Host "Installation de Google Chrome via winget..." -ForegroundColor Cyan
        winget install -e --id Google.Chrome --accept-package-agreements --accept-source-agreements
    } else {
        Write-Warning "winget non disponible. Installe Google Chrome manuellement puis relance ce script."
    }
}

function Ensure-Ollama-And-Model {
    if ($NoOllama) {
        Write-Host "Skip installation de l'IA locale (option -NoOllama)." -ForegroundColor Yellow
        return
    }

    # Tu peux mettre use_llm=false ou provider=Local dans la config pour activer l'usage du modèle.
    if (-not (Get-Command "ollama" -ErrorAction SilentlyContinue)) {
        Write-Host "Ollama non trouvé." -ForegroundColor Yellow

        if (Get-Command winget -ErrorAction SilentlyContinue) {
            Write-Host "Installation de Ollama via winget..." -ForegroundColor Cyan
            winget install -e --id Ollama.Ollama --accept-package-agreements --accept-source-agreements
        } else {
            Write-Warning "winget non disponible. Installe Ollama manuellement (ollama.com) si tu veux utiliser le LLM local."
            return
        }
    }

    if (Get-Command "ollama" -ErrorAction SilentlyContinue) {
        Write-Host "Téléchargement du modèle 'mistral:7b' pour le scoring local..." -ForegroundColor Cyan
        ollama pull mistral:7b
    }
}

function Create-RunBat {
    Write-Host "Création du fichier run_app.bat..." -ForegroundColor Cyan

    $batContent = @"
@echo off
REM Lancer l'application Gestion Emploi
cd /d "%~dp0"
call .venv\Scripts\activate
python run.py
pause
"@

    Set-Content -Path "run_app.bat" -Value $batContent -Encoding ASCII
    Write-Host "Fichier run_app.bat créé. Double-clique dessus pour lancer l'app." -ForegroundColor Green
}

# ================== MAIN ==================

Write-Host ""
Write-Host "Étape 1/4 : Vérification / installation de Python..." -ForegroundColor Cyan
Ensure-Python

Write-Host ""
Write-Host "Étape 2/4 : Environnement virtuel et dépendances..." -ForegroundColor Cyan
Ensure-Venv-And-Dependencies

Write-Host ""
Write-Host "Étape 3/4 : Navigateur Chrome pour Selenium..." -ForegroundColor Cyan
Ensure-Chrome

Write-Host ""
Write-Host "Étape 4/4 : LLM local (Ollama + mistral:7b)..." -ForegroundColor Cyan
Ensure-Ollama-And-Model

Create-RunBat

Write-Host ""
Write-Host "=== Installation terminée ===" -ForegroundColor Green
Write-Host "Utilisation : double-clique sur run_app.bat pour lancer l'application."
