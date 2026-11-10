FROM eclipse-temurin:25-jdk AS build

WORKDIR /app

COPY . .
RUN chmod +x gradlew && ./gradlew --no-daemon installDist

FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=build /app/build/install/track-my-market/ ./

# Expose web dashboard port
EXPOSE 8080

# Recommended JVM flags for Aeron IPC performance and Java 25 native memory access
ENV JAVA_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --enable-native-access=ALL-UNNAMED -XX:+UseG1GC"

# Default environment variables
ENV MARKET_SYMBOLS="RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS"
ENV MARKET_POLL_INTERVAL_MS="3000"
ENV MARKET_UI_ENABLED="true"
ENV MARKET_SERVER_PORT="8080"
ENV MARKET_SERVER_BIND_HOST="0.0.0.0"
ENV MARKET_SERVER_CONTEXT_PATH="/"
ENV YAHOO_FINANCE_BASE_URL="https://query1.finance.yahoo.com"

# Ensure execution permission for the startup script and install curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* && chmod +x bin/track-my-market

ENTRYPOINT ["bin/track-my-market"]
CMD ["--enable-ui"]
