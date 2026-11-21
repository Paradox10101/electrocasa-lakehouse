param(
    [Parameter(Mandatory = $true)][ValidateSet("1", "2", "3")][string]$Lote,
    [Parameter(Mandatory = $true)][string]$Datos,
    [ValidatePattern("^[A-Za-z0-9_]+$")][string]$Catalogo = "electrocasa_dev"
)

$ErrorActionPreference = "Stop"
$volumen = "dbfs:/Volumes/$Catalogo/default/vol_landing"

$archivos = @{
    "1" = @(
        "ventas/ventas_lote_01_2026-05-31.csv",
        "empleados/empleados_lote_01_2026-05-31.csv",
        "devoluciones/devoluciones_lote_01_2026-05-31.csv",
        "resenas/resenas_lote_01_2026-07-31.json",
        "catalogo/catalogo_productos_2026-03-01.json",
        "sucursales.csv"
    )
    "2" = @(
        "ventas/ventas_lote_02_2026-08-31.csv",
        "empleados/empleados_lote_02_2026-08-31.csv",
        "devoluciones/devoluciones_lote_02_2026-08-31.csv",
        "resenas/resenas_lote_02_2026-10-31.json",
        "catalogo/catalogo_productos_2026-08-01.json"
    )
    "3" = @(
        "ventas/ventas_lote_03_2026-10-31.csv",
        "empleados/empleados_lote_03_2026-10-31.csv",
        "devoluciones/devoluciones_lote_03_2026-10-31.csv"
    )
}

foreach ($relativo in $archivos[$Lote]) {
    $origen = Join-Path $Datos $relativo
    if (-not (Test-Path $origen)) {
        throw "No existe el archivo $origen"
    }
    $nombre = Split-Path $relativo -Leaf
    $carpeta = if ($relativo -eq "sucursales.csv") { "sucursales" } else { Split-Path $relativo -Parent }
    Write-Host "Subiendo $relativo a $volumen/$carpeta/"
    databricks fs cp $origen "$volumen/$carpeta/$nombre" --overwrite
    if ($LASTEXITCODE -ne 0) {
        throw "Falló la copia de $relativo"
    }
}

Write-Host "Lote $Lote subido a $volumen"
