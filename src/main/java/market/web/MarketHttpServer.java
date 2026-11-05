package market.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * Embedded HTTP server for track-my-market providing:
 * - Static UI dashboard serving (index.html).
 * - Server-Sent Events (SSE) streaming (/api/stream).
 * - Dynamic watchlist management REST endpoints (/api/symbols).
 */
public class MarketHttpServer {

    private static final String FALLBACK_HTML = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Track My Market</title>
        </head>
        <body>
            <h1>Track My Market</h1>
        </body>
        </html>
        """;

    private final HttpServer server;
    private final String contextPath;
    private final MarketStreamBroadcaster broadcaster;
    private final DynamicWatchlistController watchlistController;

    public MarketHttpServer(
        String bindHost,
        int port,
        String contextPath,
        MarketStreamBroadcaster broadcaster,
        DynamicWatchlistController watchlistController
    ) throws IOException {
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster must not be null");
        this.watchlistController = Objects.requireNonNull(watchlistController, "watchlistController must not be null");
        this.contextPath = normalizeContextPath(contextPath);

        this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        configureRoutes();
    }

    public static String normalizeContextPath(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String trimmed = raw.trim();
        if (trimmed.equals("/")) {
            return "/";
        }
        if (!trimmed.startsWith("/")) {
            trimmed = "/" + trimmed;
        }
        while (trimmed.length() > 1 && trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private String fullPath(String subPath) {
        if (contextPath.isEmpty() || contextPath.equals("/")) {
            return subPath.startsWith("/") ? subPath : "/" + subPath;
        }
        return contextPath + (subPath.startsWith("/") ? subPath : "/" + subPath);
    }

    private void configureRoutes() {
        String baseContext = (contextPath.isEmpty() || contextPath.equals("/")) ? "/" : contextPath;

        server.createContext(fullPath("/api/stream"), this::handleStream);
        server.createContext(fullPath("/api/symbols"), this::handleSymbols);
        server.createContext(fullPath("/api/search"), this::handleSearch);
        server.createContext(fullPath("/api/chart"), this::handleChart);
        server.createContext(baseContext, this::handleStatic);
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String reqPath = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        String rootPath = fullPath("/");
        String indexFilePath = fullPath("/index.html");
        String baseNoSlash = (contextPath.length() > 1 && contextPath.endsWith("/"))
            ? contextPath.substring(0, contextPath.length() - 1)
            : contextPath;

        boolean isIndex = reqPath.equals(rootPath)
            || reqPath.equals(indexFilePath)
            || (!baseNoSlash.isEmpty() && reqPath.equals(baseNoSlash));

        if (!isIndex) {
            byte[] notFound = "Not Found".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, notFound.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(notFound);
            }
            return;
        }

        if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        byte[] htmlBytes;
        try (InputStream in = getClass().getResourceAsStream("/web/index.html")) {
            if (in != null) {
                htmlBytes = in.readAllBytes();
            } else {
                htmlBytes = FALLBACK_HTML.getBytes(StandardCharsets.UTF_8);
            }
        }

        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, htmlBytes.length);

        if (!"HEAD".equalsIgnoreCase(method)) {
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(htmlBytes);
            }
        } else {
            exchange.getResponseBody().close();
        }
    }

    private void handleStream(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=UTF-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache, no-transform");
        exchange.getResponseHeaders().set("Connection", "keep-alive");
        exchange.getResponseHeaders().set("X-Accel-Buffering", "no");

        // 0 enables chunked transfer encoding for streaming
        exchange.sendResponseHeaders(200, 0);

        OutputStream os = exchange.getResponseBody();
        String initialSnapshot = broadcaster.getInitialSnapshotJson();
        String snapshotMsg = "event: initial-snapshot\ndata: " + initialSnapshot + "\n\n";
        os.write(snapshotMsg.getBytes(StandardCharsets.UTF_8));
        os.flush();

        broadcaster.registerClient(os);
    }

    private void handleSymbols(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        String path = exchange.getRequestURI().getPath();
        String symbolsBase = fullPath("/api/symbols");

        try {
            switch (method) {
                case "GET" -> {
                    if (path.equals(symbolsBase + "/status") || path.equals(symbolsBase + "/status/")) {
                        String json = watchlistController.handleGetSymbolStatusJson();
                        sendJson(exchange, 200, json);
                        return;
                    }
                    if (!path.equals(symbolsBase) && !path.equals(symbolsBase + "/")) {
                        sendError(exchange, 404, "Not Found");
                        return;
                    }
                    String json = watchlistController.handleGetSymbolsJson();
                    sendJson(exchange, 200, json);
                }
                case "POST" -> {
                    if (!path.equals(symbolsBase) && !path.equals(symbolsBase + "/")) {
                        sendError(exchange, 404, "Not Found");
                        return;
                    }
                    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    String json = watchlistController.handleAddSymbolJson(body);
                    sendJson(exchange, 200, json);
                }
                case "DELETE" -> {
                    String prefix = symbolsBase + "/";
                    if (!path.startsWith(prefix) || path.length() <= prefix.length()) {
                        sendError(exchange, 400, "Missing symbol in delete path");
                        return;
                    }
                    String rawSymbol = path.substring(prefix.length());
                    if (rawSymbol.endsWith("/")) {
                        rawSymbol = rawSymbol.substring(0, rawSymbol.length() - 1);
                    }
                    String symbol = URLDecoder.decode(rawSymbol, StandardCharsets.UTF_8);
                    String json = watchlistController.handleRemoveSymbolJson(symbol);
                    sendJson(exchange, 200, json);
                }
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                }
            }
        } catch (IllegalArgumentException e) {
            sendError(exchange, 400, e.getMessage());
        }
    }

    private void handleSearch(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        if (!"GET".equals(method)) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String searchBase = fullPath("/api/search");
        if (!path.equals(searchBase) && !path.equals(searchBase + "/")) {
            sendError(exchange, 404, "Not Found");
            return;
        }

        String query = exchange.getRequestURI().getQuery();
        String searchTerm = "";
        if (query != null) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=", 2);
                if (pair.length == 2 && "q".equalsIgnoreCase(pair[0])) {
                    searchTerm = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
                    break;
                }
            }
        }

        String json = watchlistController.handleSearchJson(searchTerm);
        sendJson(exchange, 200, json);
    }

    private void handleChart(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        if (!"GET".equals(method)) {
            sendError(exchange, 405, "Method Not Allowed");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String chartBase = fullPath("/api/chart");
        if (!path.equals(chartBase) && !path.equals(chartBase + "/")) {
            sendError(exchange, 404, "Not Found");
            return;
        }

        String query = exchange.getRequestURI().getQuery();
        String symbol = "";
        String timeframe = "1d";
        if (query != null) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=", 2);
                if (pair.length == 2) {
                    if ("symbol".equalsIgnoreCase(pair[0])) {
                        symbol = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
                    } else if ("timeframe".equalsIgnoreCase(pair[0])) {
                        timeframe = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
                    }
                }
            }
        }

        String json = watchlistController.handleGetChartJson(symbol, timeframe);
        sendJson(exchange, 200, json);
    }

    private void sendJson(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String errorJson = "{\"error\":\"" + escapeJson(message) + "\"}";
        sendJson(exchange, statusCode, errorJson);
    }

    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public void stop(int delaySeconds) {
        server.stop(delaySeconds);
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public String getContextPath() {
        return contextPath;
    }
}
