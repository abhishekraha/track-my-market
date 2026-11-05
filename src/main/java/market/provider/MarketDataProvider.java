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
     * Unsubscribes from market data for the given symbol.
     *
     * @param symbol ticker symbol to remove
     */
    default void unsubscribe(String symbol) {
    }

    /**
     * Returns the list of currently subscribed ticker symbols.
     *
     * @return list of active ticker symbols
     */
    default List<String> getSubscribedSymbols() {
        return List.of();
    }

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
