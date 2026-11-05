package market.bus.aeron;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import market.bus.MarketEventBus;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron and Archiving Media Driver backed implementation of {@link MarketEventBus}.
 */
public class AeronEventBus implements MarketEventBus {

    public static final String DEFAULT_CHANNEL = CommonContext.IPC_CHANNEL;
    public static final int DEFAULT_STREAM_ID = 1001;
    public static final int DEFAULT_MAX_OFFER_RETRIES = 100_000;

    private final String channel;
    private final int defaultStreamId;
    private final File mediaDriverDir;
    private final File archiveDir;
    private final boolean deleteDirsOnClose;
    private final IdleStrategy idleStrategy;
    private final int maxOfferRetries;

    private final ConcurrentMap<Integer, Publication> publications = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, List<FragmentHandler>> handlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, FragmentHandler> cachedCompositeHandlers = new ConcurrentHashMap<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ArchivingMediaDriver archivingMediaDriver;
    private Aeron aeron;

    public AeronEventBus() {
        this(DEFAULT_CHANNEL, DEFAULT_STREAM_ID);
    }

    public AeronEventBus(String channel) {
        this(channel, DEFAULT_STREAM_ID);
    }

    public AeronEventBus(String channel, int defaultStreamId) {
        this(channel, defaultStreamId, null, null, true, new YieldingIdleStrategy(), DEFAULT_MAX_OFFER_RETRIES);
    }

    public AeronEventBus(
            String channel,
            int defaultStreamId,
            File mediaDriverDir,
            File archiveDir,
            boolean deleteDirsOnClose,
            IdleStrategy idleStrategy,
            int maxOfferRetries) {
        this.channel = (channel != null) ? channel : DEFAULT_CHANNEL;
        this.defaultStreamId = defaultStreamId;
        this.mediaDriverDir = (mediaDriverDir != null)
                ? mediaDriverDir
                : new File(CommonContext.generateRandomDirName());
        this.archiveDir = (archiveDir != null)
                ? archiveDir
                : new File(this.mediaDriverDir.getAbsolutePath() + "-archive");
        this.deleteDirsOnClose = deleteDirsOnClose;
        this.idleStrategy = (idleStrategy != null) ? idleStrategy : new YieldingIdleStrategy();
        this.maxOfferRetries = maxOfferRetries > 0 ? maxOfferRetries : DEFAULT_MAX_OFFER_RETRIES;
    }

    @Override
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("Cannot start closed AeronEventBus");
        }
        if (started.get()) {
            return;
        }

        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(mediaDriverDir.getAbsolutePath())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED);

        Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(mediaDriverDir.getAbsolutePath())
                .archiveDir(archiveDir)
                .controlChannel("aeron:udp?endpoint=localhost:0")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED);

        archivingMediaDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);

        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriverDir.getAbsolutePath());
        aeron = Aeron.connect(aeronCtx);

        // Pre-create publication and subscription for defaultStreamId
        publications.computeIfAbsent(defaultStreamId, s -> aeron.addPublication(channel, s));
        subscriptions.computeIfAbsent(defaultStreamId, s -> aeron.addSubscription(channel, s));

        // Create subscriptions for any streamIds registered prior to start()
        for (Integer streamId : handlers.keySet()) {
            subscriptions.computeIfAbsent(streamId, s -> aeron.addSubscription(channel, s));
        }

        started.set(true);
    }

    @Override
    public boolean publish(int streamId, DirectBuffer buffer, int offset, int length) {
        if (!started.get() || closed.get()) {
            return false;
        }

        Publication publication = publications.computeIfAbsent(streamId, s -> aeron.addPublication(channel, s));
        if (publication == null || publication.isClosed()) {
            return false;
        }

        for (int i = 0; i < maxOfferRetries; i++) {
            if (closed.get()) {
                return false;
            }

            long result = publication.offer(buffer, offset, length);
            if (result >= 0) {
                return true;
            }

            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                return false;
            }

            if (result == Publication.BACK_PRESSURED
                    || result == Publication.ADMIN_ACTION
                    || result == Publication.NOT_CONNECTED) {
                idleStrategy.idle();
                continue;
            }

            idleStrategy.idle();
        }

        return false;
    }

    @Override
    public void subscribe(int streamId, FragmentHandler handler) {
        if (closed.get()) {
            throw new IllegalStateException("Cannot subscribe to closed AeronEventBus");
        }
        if (handler == null) {
            throw new IllegalArgumentException("FragmentHandler cannot be null");
        }

        List<FragmentHandler> streamHandlers = handlers.computeIfAbsent(streamId, k -> new CopyOnWriteArrayList<>());
        streamHandlers.add(handler);
        updateCompositeHandler(streamId, streamHandlers);

        if (started.get() && aeron != null) {
            subscriptions.computeIfAbsent(streamId, s -> aeron.addSubscription(channel, s));
        }
    }

    private void updateCompositeHandler(int streamId, List<FragmentHandler> streamHandlers) {
        if (streamHandlers.isEmpty()) {
            cachedCompositeHandlers.remove(streamId);
        } else if (streamHandlers.size() == 1) {
            cachedCompositeHandlers.put(streamId, streamHandlers.get(0));
        } else {
            cachedCompositeHandlers.put(streamId, (buffer, offset, length, header) -> {
                for (int i = 0; i < streamHandlers.size(); i++) {
                    streamHandlers.get(i).onFragment(buffer, offset, length, header);
                }
            });
        }
    }

    @Override
    public int poll(int fragmentLimit) {
        if (!started.get() || closed.get()) {
            return 0;
        }

        int totalPolled = 0;
        for (Subscription subscription : subscriptions.values()) {
            if (subscription.isClosed()) {
                continue;
            }
            int remainingLimit = fragmentLimit - totalPolled;
            if (remainingLimit <= 0) {
                break;
            }

            FragmentHandler handler = cachedCompositeHandlers.get(subscription.streamId());
            if (handler != null) {
                totalPolled += subscription.poll(handler, remainingLimit);
            }
        }

        return totalPolled;
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        started.set(false);

        try {
            CloseHelper.closeAll(publications.values());
            publications.clear();
        } catch (Throwable ignored) {
        }

        try {
            CloseHelper.closeAll(subscriptions.values());
            subscriptions.clear();
        } catch (Throwable ignored) {
        }

        try {
            CloseHelper.close(aeron);
            aeron = null;
        } catch (Throwable ignored) {
        }

        try {
            CloseHelper.close(archivingMediaDriver);
            archivingMediaDriver = null;
        } catch (Throwable ignored) {
        }

        if (deleteDirsOnClose) {
            if (archiveDir != null && archiveDir.exists()) {
                IoUtil.delete(archiveDir, true);
            }
            if (mediaDriverDir != null && mediaDriverDir.exists()) {
                IoUtil.delete(mediaDriverDir, true);
            }
        }
    }

    public String channel() {
        return channel;
    }

    public int defaultStreamId() {
        return defaultStreamId;
    }

    public File mediaDriverDir() {
        return mediaDriverDir;
    }

    public File archiveDir() {
        return archiveDir;
    }

    public boolean isStarted() {
        return started.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public ArchivingMediaDriver archivingMediaDriver() {
        return archivingMediaDriver;
    }

    public Aeron aeron() {
        return aeron;
    }
}
