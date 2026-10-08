# Simulador pequeno del sistema de cuentas

Este programa representa el servicio que el motor de transacciones espera en
`http://localhost:8081`. Devuelve respuestas fijas: todas las cuentas son validas,
siempre hay fondos y todas las transferencias se reportan como exitosas.
No guarda cuentas ni cambia saldos.

Se ejecuta como un proceso Java separado y usa el servidor HTTP incluido en el
[JDK](https://docs.oracle.com/en/java/javase/17/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html).
No necesita Maven, PostgreSQL, Redis ni librerias adicionales.

## 1. Iniciar el simulador

Desde la raiz del repositorio, abre una terminal PowerShell y ejecuta:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\account-simulator\start.ps1
```

El script usa `JAVA_HOME` si esta configurado. Si no, busca el Java de IntelliJ
en la ruta usada en este reto y luego el Java disponible en el PATH.
Manten esta terminal abierta. Puedes detener el simulador con `Ctrl+C`.
El launcher utiliza `C:\tmp` (o `tmp` en la unidad del sistema) como directorio
temporal de sockets para evitar un problema del JDK con las rutas temporales
de este Windows. La configuracion solo afecta al proceso del simulador.

## 2. Comprobar las respuestas fijas

En otra terminal:

```powershell
curl.exe -i http://localhost:8081/api/accounts/10001/validate
curl.exe -i "http://localhost:8081/api/accounts/10001/funds?amount=100"
curl.exe -i -X POST http://localhost:8081/api/accounts/transfer
```

| Peticion | HTTP | Respuesta |
|---|---|---|
| GET `/api/accounts/{cuenta}/validate` | 200 | `{"valid":true}` |
| GET `/api/accounts/{cuenta}/funds?amount=100` | 200 | `{"sufficient":true}` |
| POST `/api/accounts/transfer` | 200 | `{"success":true}` |

La cuenta y el monto pueden variar; las respuestas son las mismas.
Las rutas desconocidas devuelven 404 y un metodo incorrecto devuelve 405.
Cada solicitud aparece en la consola del simulador.

## 3. Usarlo desde el motor de transacciones

Manten el motor de Spring Boot en `localhost:8080` y este simulador en
`localhost:8081`. El cliente `AccountSystemWebClient` ya apunta por defecto a
esta direccion. Asi puedes enviar un POST al motor y observar sus llamadas
en la consola del simulador.

Si configuraste otra direccion para el sistema de cuentas, puedes indicar esta
antes de iniciar Spring Boot:

```powershell
$env:ACCOUNT_SYSTEM_BASE_URL = "http://localhost:8081"
```

Que el simulador responda correctamente verifica el servicio ficticio. El
procesamiento, la idempotencia y el manejo de errores del motor son parte del reto.

## Simular una demora

Deten el simulador con `Ctrl+C` y vuelvelo a iniciar con una demora de tres
segundos en cada respuesta:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\account-simulator\start.ps1 -DelayMs 3000
```

La demora permite observar como reacciona el motor cuando el sistema de cuentas
tarda en responder. Los reintentos del motor pueden hacer que la duracion total
de una solicitud sea mayor que la demora de una sola respuesta.

Para volver a respuestas inmediatas, reinicia el simulador sin `-DelayMs`.
Puedes cambiar su puerto mediante `-Port 8082`; en ese caso tambien ajusta
`ACCOUNT_SYSTEM_BASE_URL` antes de arrancar el motor.
