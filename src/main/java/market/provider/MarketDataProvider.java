package market.provider;

import java.util.List;
import java.util.function.Consumer;

/**
 * Pluggable market data provider contract for streaming and polled market quotes.
 */
public interface MarketDataProvider extends AutoCloseable {

    /**
     * Starts the ingestion loop / connection.
     */
    void start();

    /**
     * Stops the ingestion loop / connection.
     */
    void stop();

    /**
     * Subscribes to market data for the given symbols.
     *
     * @param symbols list of ticker symbols
     */
    void subscribe(List<String> symbols);

    /**
     * Registers a callback listener to receive parsed market quotes.
     *
     * @param listener consumer receiving quotes
     */
    void registerTickListener(Consumer<RawMarketQuote> listener);

    @Override
    default void close() {
        stop();
    }
}
