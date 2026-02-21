param(
    [string]$AppName = "RemoteControlHub"
)

if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    Write-Error "PyInstaller не найден. Установите зависимости: pip install -r requirements.txt"
    exit 1
}

$arguments = @(
    "--noconfirm",
    "--clean",
    "--windowed",
    "--name", $AppName,
    "--add-data", "README.md;.",
    "main.py"
)

Write-Host "Сборка EXE: pyinstaller $($arguments -join ' ')" -ForegroundColor Cyan
pyinstaller @arguments

if ($LASTEXITCODE -eq 0) {
    Write-Host "Готово: dist\$AppName\$AppName.exe" -ForegroundColor Green
    exit 0
}

Write-Error "Сборка завершилась с ошибкой."
exit 1
