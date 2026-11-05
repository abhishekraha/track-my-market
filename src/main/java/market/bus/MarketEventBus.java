package market.bus;

import org.agrona.DirectBuffer;
import io.aeron.logbuffer.FragmentHandler;

public interface MarketEventBus extends AutoCloseable {
    void start();
    boolean publish(int streamId, DirectBuffer buffer, int offset, int length);
    void subscribe(int streamId, FragmentHandler handler);
    int poll(int fragmentLimit);
    @Override
    void close();
}
