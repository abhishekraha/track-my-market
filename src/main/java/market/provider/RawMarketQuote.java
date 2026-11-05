package market.provider;

/**
 * Immutable raw market quote model emitted by {@link MarketDataProvider} implementations.
 *
 * @param symbol exchange ticker symbol (e.g. RELIANCE.NS)
 * @param price regular market price
 * @param quoteTimestampEpochNs quote timestamp from exchange / source in Unix epoch nanoseconds
 * @param fetchDurationNs round-trip network fetch latency in nanoseconds
 * @param isin 12-character International Securities Identification Number
 * @param sedol 7-character Stock Exchange Daily Official List code
 * @param ric Reuters Instrument Code
 */
public record RawMarketQuote(
    String symbol,
    double price,
    long quoteTimestampEpochNs,
    long fetchDurationNs,
    String isin,
    String sedol,
    String ric
) {
}
