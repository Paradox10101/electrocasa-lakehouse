param(
    [string]$GrupoRecursos = "rg-electrocasa",
    [string]$Workspace = "dbw-electrocasa"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Intentar {
    param([string]$Descripcion, [scriptblock]$Comando)
    $anterior = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Comando
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Aviso: no se pudo $Descripcion (código $LASTEXITCODE)" -ForegroundColor Yellow
    }
    $ErrorActionPreference = $anterior
}

Write-Host "Esto borra los jobs, los pipelines, los catálogos electrocasa_dev y electrocasa_prod, la Connection,"
Write-Host "el workspace $Workspace y el grupo de recursos $GrupoRecursos. No se puede deshacer."
if ((Read-Host "Escribe BORRAR para seguir") -ne "BORRAR") {
    Write-Host "Cancelado"
    exit 0
}

$ErrorActionPreference = "Continue"
$url = az databricks workspace show --name $Workspace --resource-group $GrupoRecursos --query workspaceUrl --output tsv 2>$null
$encontrado = ($LASTEXITCODE -eq 0 -and $url)
$ErrorActionPreference = "Stop"
if ($encontrado) {
    $env:DATABRICKS_HOST = "https://$url"
    if (-not $env:BUNDLE_VAR_correo_alertas) { $env:BUNDLE_VAR_correo_alertas = "sin-uso@example.com" }
    if (-not $env:BUNDLE_VAR_bd_tracking) { $env:BUNDLE_VAR_bd_tracking = "sin_uso" }
    foreach ($objetivo in "dev", "prod") {
        Intentar "destruir el bundle de $objetivo" { databricks bundle destroy -t $objetivo --auto-approve }
    }
    foreach ($catalogo in "electrocasa_dev", "electrocasa_prod") {
        Intentar "borrar el catálogo $catalogo" { databricks catalogs delete $catalogo --force }
    }
    Intentar "borrar la Connection" { databricks connections delete electrocasa_tracking_sqlserver }
}

Intentar "borrar el grupo de recursos" { az group delete --name $GrupoRecursos --yes }
Write-Host "Recursos borrados. Revisa el portal de Azure para confirmar que el grupo $GrupoRecursos ya no existe." -ForegroundColor Green
