param(
    [Parameter(Mandatory = $true)][string]$CorreoAlertas,
    [Parameter(Mandatory = $true)][string]$Datos,
    [ValidateRange(1, 9)][int]$Desde = 1,
    [string]$Region = "westus2",
    [string]$GrupoRecursos = "rg-electrocasa",
    [string]$Workspace = "dbw-electrocasa"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$Conexion = "electrocasa_tracking_sqlserver"

function Nativo {
    param([string]$Descripcion, [scriptblock]$Comando)
    & $Comando
    if ($LASTEXITCODE -ne 0) {
        throw "Falló: $Descripcion (código $LASTEXITCODE)"
    }
}

function Existe {
    param([string]$Programa, [string[]]$Argumentos)
    $anterior = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Programa @Argumentos 2>&1 | Out-Null
    $codigo = $LASTEXITCODE
    $ErrorActionPreference = $anterior
    return ($codigo -eq 0)
}

function Paso {
    param([int]$Numero, [string]$Descripcion, [scriptblock]$Accion)
    if ($Numero -lt $Desde) {
        return
    }
    Write-Host ""
    Write-Host "==> Paso $Numero. $Descripcion" -ForegroundColor Cyan
    & $Accion
}

function Crear-Catalogo {
    param([string]$Catalogo)
    if (-not (Existe "databricks" @("catalogs", "get", $Catalogo))) {
        Nativo "crear el catálogo $Catalogo" { databricks catalogs create $Catalogo }
    }
    foreach ($esquema in "bronze", "silver", "gold") {
        if (-not (Existe "databricks" @("schemas", "get", "$Catalogo.$esquema"))) {
            Nativo "crear el schema $Catalogo.$esquema" { databricks schemas create $esquema $Catalogo }
        }
    }
    if (-not (Existe "databricks" @("volumes", "read", "$Catalogo.default.vol_landing"))) {
        Nativo "crear el volume de $Catalogo" { databricks volumes create $Catalogo default vol_landing MANAGED }
    }
}

if (-not (Existe "az" @("account", "show"))) {
    Nativo "iniciar sesión en Azure" { az login --only-show-errors }
}
Nativo "instalar la extensión databricks del CLI de Azure" { az extension add --name databricks --upgrade --only-show-errors }

$env:BUNDLE_VAR_correo_alertas = $CorreoAlertas
if (-not $env:BUNDLE_VAR_bd_tracking) {
    $env:BUNDLE_VAR_bd_tracking = Read-Host "Nombre de la base de datos de Tracking"
}

Paso 1 "Grupo de recursos y workspace serverless" {
    Nativo "crear el grupo de recursos" { az group create --name $GrupoRecursos --location $Region --only-show-errors --output none }
    if (-not (Existe "az" @("databricks", "workspace", "show", "--name", $Workspace, "--resource-group", $GrupoRecursos))) {
        Nativo "crear el workspace" {
            az databricks workspace create --name $Workspace --resource-group $GrupoRecursos --location $Region --sku premium --compute-mode Serverless --only-show-errors --output none
        }
    }
}

$url = az databricks workspace show --name $Workspace --resource-group $GrupoRecursos --query workspaceUrl --output tsv
if ($LASTEXITCODE -ne 0 -or -not $url) {
    throw "No se encontró el workspace $Workspace; corre el script desde el paso 1"
}
$env:DATABRICKS_HOST = "https://$url"
Write-Host "Workspace: $env:DATABRICKS_HOST"
Nativo "autenticar el CLI de Databricks con la sesión de Azure" { databricks current-user me --output json | Out-Null }

Paso 2 "Grupos (en la interfaz)" {
    Write-Host "Abre $env:DATABRICKS_HOST > Settings > Identity and access > Groups y crea:"
    Write-Host "  electrocasa_ingenieria, electrocasa_analistas y electrocasa_auditoria"
    Write-Host "Después agrégate como miembro de electrocasa_ingenieria."
    Read-Host "Presiona Enter cuando estén creados" | Out-Null
}

Paso 3 "Connection de Unity Catalog hacia Tracking" {
    if (Existe "databricks" @("connections", "get", $Conexion)) {
        Write-Host "La Connection $Conexion ya existe"
        return
    }
    $servidor = Read-Host "Servidor de la base de Tracking"
    $usuario = Read-Host "Usuario de solo lectura"
    $clave = Read-Host "Contraseña" -AsSecureString
    $cuerpo = @{
        name            = $Conexion
        connection_type = "SQLSERVER"
        options         = @{
            host     = $servidor
            port     = "1433"
            user     = $usuario
            password = [System.Net.NetworkCredential]::new("", $clave).Password
        }
    } | ConvertTo-Json -Compress
    $archivo = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($archivo, $cuerpo)
        Nativo "crear la Connection" { databricks connections create --json "@$archivo" --output json | Out-Null }
        Write-Host "Connection $Conexion creada"
    }
    finally {
        Remove-Item $archivo -Force -ErrorAction SilentlyContinue
        $cuerpo = $null
    }
}

Paso 4 "Catálogo, schemas y volume de dev" { Crear-Catalogo "electrocasa_dev" }

Paso 5 "Validar y desplegar el bundle en dev" {
    Nativo "validar el bundle" { databricks bundle validate -t dev }
    Nativo "desplegar el bundle" { databricks bundle deploy -t dev }
}

Paso 6 "Setup: máscaras, permisos y demostración de REVOKE" {
    Nativo "correr el setup" { databricks bundle run -t dev electrocasa_setup }
}

Paso 7 "Los tres lotes, uno por uno" {
    foreach ($lote in "1", "2", "3") {
        & (Join-Path $PSScriptRoot "subir_lote.ps1") -Lote $lote -Datos $Datos -Catalogo "electrocasa_dev"
        Nativo "correr el job con el lote $lote" { databricks bundle run -t dev electrocasa_job }
    }
}

Paso 8 "Segunda corrida del conector y monitoreo" {
    Nativo "volver a correr el conector" { databricks bundle run -t dev electrocasa_tracking }
    Nativo "correr el monitoreo" { databricks bundle run -t dev electrocasa_monitoreo }
}

Paso 9 "Despliegue de prod" {
    Crear-Catalogo "electrocasa_prod"
    Nativo "desplegar prod" { databricks bundle deploy -t prod }
    Nativo "correr el setup de prod" { databricks bundle run -t prod electrocasa_setup }
}

Write-Host ""
Write-Host "Listo. Las corridas están en $env:DATABRICKS_HOST > Jobs & Pipelines." -ForegroundColor Green
