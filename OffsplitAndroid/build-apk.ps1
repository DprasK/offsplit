$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot

if (Test-Path -LiteralPath ".\gradlew.bat") {
    .\gradlew.bat assembleDebug
    exit $LASTEXITCODE
}

$gradle = Get-Command gradle -ErrorAction SilentlyContinue
if ($null -ne $gradle) {
    gradle assembleDebug
    exit $LASTEXITCODE
}

Write-Host "Gradle belum ditemukan."
Write-Host "Buka folder ini di Android Studio dulu: $PSScriptRoot"
Write-Host "Setelah Gradle sync selesai, build APK dari menu Build > Build Bundle(s) / APK(s) > Build APK(s)."
exit 1
