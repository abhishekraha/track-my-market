package market.web;

import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicWatchlistControllerTest {

    @Test
    void shouldAddAndRemoveSymbolsDynamically() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS", "TCS.NS"));

        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        String initialSymbols = controller.handleGetSymbolsJson();
        assertThat(initialSymbols).contains("RELIANCE.NS", "TCS.NS");

        String addedResponse = controller.handleAddSymbolJson("{\"symbol\":\"INFY.NS\"}");
        assertThat(addedResponse).contains("INFY.NS");
        assertThat(provider.getSubscribedSymbols()).contains("INFY.NS");

        String removedResponse = controller.handleRemoveSymbolJson("TCS.NS");
        assertThat(removedResponse).doesNotContain("TCS.NS");
        assertThat(provider.getSubscribedSymbols()).doesNotContain("TCS.NS");
    }

    @Test
    void shouldNormalizeSymbolsToUppercaseAndTrimWhitespace() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        String addedResponse = controller.handleAddSymbolJson("{\"symbol\": \"  wipro.ns  \"}");
        assertThat(addedResponse).contains("WIPRO.NS");
        assertThat(provider.getSubscribedSymbols()).contains("WIPRO.NS");

        String removedResponse = controller.handleRemoveSymbolJson("  wipro.ns  ");
        assertThat(removedResponse).doesNotContain("WIPRO.NS");
        assertThat(provider.getSubscribedSymbols()).doesNotContain("WIPRO.NS");
    }

    @Test
    void shouldRejectInvalidSymbolFormat() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        assertThatThrownBy(() -> controller.handleAddSymbolJson("{\"symbol\": \"BAD SYMBOL!@#\"}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid symbol");

        assertThatThrownBy(() -> controller.handleRemoveSymbolJson("BAD SYMBOL!@#"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid symbol");
    }

    @Test
    void shouldRejectEmptyOrMalformedJsonPayload() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        assertThatThrownBy(() -> controller.handleAddSymbolJson(null))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> controller.handleAddSymbolJson(""))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> controller.handleAddSymbolJson("{\"other\": \"value\"}"))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> controller.handleAddSymbolJson("{broken json"))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> controller.handleAddSymbolJson("{\"symbol\": \"\"}"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldReturnSymbolStatusesWithErrors() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS", "TATAMOTORS.NS"));
        provider.handleSymbolErrorResponse("TATAMOTORS.NS", "delisted");

        DynamicWatchlistController controller = new DynamicWatchlistController(provider);
        String statusJson = controller.handleGetSymbolStatusJson();

        assertThat(statusJson).contains("\"RELIANCE.NS\":{\"status\":\"OK\"");
        assertThat(statusJson).contains("\"companyName\":\"Reliance Industries Limited\"");
        assertThat(statusJson).contains("\"isTradable\":");
        assertThat(statusJson).contains("\"TATAMOTORS.NS\":{\"status\":\"ERROR\"");
        assertThat(statusJson).contains("TMPV.NS");
    }

    @Test
    void shouldReturnChartHistoryJsonForSymbol() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS"));
        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        String chartJson = controller.handleGetChartJson("RELIANCE.NS", "1d");
        assertThat(chartJson).contains("\"symbol\":\"RELIANCE.NS\"");
        assertThat(chartJson).contains("\"timeframe\":\"1d\"");
        assertThat(chartJson).contains("\"points\":");
    }

    @Test
    void shouldRejectNullProvider() {
        assertThatThrownBy(() -> new DynamicWatchlistController(null))
            .isInstanceOf(NullPointerException.class);
    }
}
