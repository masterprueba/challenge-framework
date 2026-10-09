param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8081,

    [ValidateRange(0, 60000)]
    [int]$DelayMs = 0,

    [ValidateRange(1, 256)]
    [int]$Threads = 4,

    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'
$taskJavaExecutable = $null

if ($env:JAVA_HOME) {
    $taskJavaFromHome = Join-Path $env:JAVA_HOME 'bin\java.exe'
    if (Test-Path -LiteralPath $taskJavaFromHome) {
        $taskJavaExecutable = $taskJavaFromHome
    }
}

if (-not $taskJavaExecutable) {
    $taskIntellijJava = 'C:\Program Files\JetBrains\IntelliJ IDEA 2025.2.3\jbr\bin\java.exe'
    if (Test-Path -LiteralPath $taskIntellijJava) {
        $taskJavaExecutable = $taskIntellijJava
    } else {
        $taskJavaCommand = Get-Command java -ErrorAction SilentlyContinue
        if ($taskJavaCommand) {
            $taskJavaExecutable = $taskJavaCommand.Source
        }
    }
}

if (-not $taskJavaExecutable) {
    throw 'No se encontro Java. Configura JAVA_HOME con el JDK utilizado para el reto.'
}

$taskSourcePath = Join-Path $PSScriptRoot 'src\com\bancadigital\simulator\AccountSimulator.java'
# Una ruta temporal corta evita el fallo de sockets locales del JDK en este Windows.
$taskSocketTemp = Join-Path $env:SystemDrive 'tmp'
if (-not (Test-Path -LiteralPath $taskSocketTemp)) {
    New-Item -ItemType Directory -Path $taskSocketTemp | Out-Null
}

& $taskJavaExecutable "-Djdk.net.unixdomain.tmpdir=$taskSocketTemp" "-Dsimulator.port=$Port" "-Dsimulator.delay-ms=$DelayMs" "-Dsimulator.threads=$Threads" "-Dsimulator.quiet=$($Quiet.IsPresent.ToString().ToLowerInvariant())" $taskSourcePath
exit $LASTEXITCODE
