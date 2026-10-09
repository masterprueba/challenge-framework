package com.bancadigital.simulator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Sistema de cuentas ficticio para practicar las llamadas HTTP del reto. */
public class AccountSimulator {

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getProperty("simulator.port", "8081"));
        int delayMs = Integer.parseInt(System.getProperty("simulator.delay-ms", "0"));
        int threads = Integer.parseInt(System.getProperty("simulator.threads", "4"));
        boolean quiet = Boolean.parseBoolean(System.getProperty("simulator.quiet", "false"));
        if (port < 1 || port > 65535 || delayMs < 0 || delayMs > 60000 || threads < 1 || threads > 256) {
            throw new IllegalArgumentException("Puerto: 1-65535. Demora: 0-60000 ms. Hilos: 1-256.");
        }

        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        server.setExecutor(executor);
        server.createContext("/", exchange -> handleRequest(exchange, delayMs, quiet));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            executor.shutdownNow();
        }));

        server.start();
        System.out.printf("Simulador de cuentas en http://localhost:%d%n", port);
        System.out.printf("Demora: %d ms. Hilos: %d. Silencioso: %s. Detener con Ctrl+C.%n",
                delayMs, threads, quiet);
    }

    private static void handleRequest(HttpExchange exchange, int delayMs, boolean quiet) {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        if (!quiet) System.out.printf("%s %s%n", method, exchange.getRequestURI());

        try {
            String expectedMethod;
            String response;
            if (path.matches("/api/accounts/[^/]+/validate")) {
                expectedMethod = "GET";
                response = "{\"valid\":true}";
            } else if (path.matches("/api/accounts/[^/]+/funds")) {
                expectedMethod = "GET";
                response = "{\"sufficient\":true}";
            } else if (path.equals("/api/accounts/transfer")) {
                expectedMethod = "POST";
                response = "{\"success\":true}";
            } else {
                sendJson(exchange, 404, "{\"error\":\"Ruta no encontrada\"}");
                return;
            }

            if (!method.equals(expectedMethod)) {
                exchange.getResponseHeaders().set("Allow", expectedMethod);
                sendJson(exchange, 405, "{\"error\":\"Metodo no permitido\"}");
                return;
            }

            // El contenido se consume, pero las respuestas son siempre fijas.
            exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
            Thread.sleep(delayMs);
            sendJson(exchange, 200, response);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException error) {
            System.err.println("Conexion cerrada durante la respuesta: " + error.getMessage());
        } finally {
            exchange.close();
        }
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }
}
