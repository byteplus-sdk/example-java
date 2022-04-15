package byteplus.example.media;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.core.Option;
import byteplus.sdk.media.MediaClient;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUsersRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUsersResponse;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteContentsRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteContentsResponse;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUserEventsRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUserEventsResponse;
import byteplus.sdk.media.protocol.ByteplusMedia.AckServerImpressionsRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.AckServerImpressionsResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConcurrentHelper {
    private final static int CORE_POOL_SIZE = 5;

    private final static int MAX_POOL_SIZE = 7;

    private final static int KEEP_ALICE_MINUTES = 5;

    private final static int MAX_BLOCK_TASK_COUNT = 20;

    private final static int RETRY_TIMES = 2;

    private final ExecutorService executor = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            KEEP_ALICE_MINUTES, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(MAX_BLOCK_TASK_COUNT),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    private final MediaClient client;

    private final RequestHelper requestHelper;

    public ConcurrentHelper(MediaClient client) {
        this.client = client;
        this.requestHelper = new RequestHelper(client);
    }

    // Submit tasks.
    // If the number of imported tasks currently executing exceeds the maximum number
    // of concurrent tasks, the commit will be blocked until other task complete.
    // Only supported for "import_xxx" and "ack_impressions" request.
    // It is recommended to increase the data amount contained in a single request.
    // It is not recommended to use too many concurrent imports,
    // which may lead to server overload and limit the flow of the request
    public void submitRequest(Object request, Option... opts) {
        Runnable run;
        if (request instanceof WriteUsersRequest) {
            run = () -> doWriteUsers((WriteUsersRequest) request, opts);
        } else if (request instanceof WriteContentsRequest) {
            run = () -> doWriteContents((WriteContentsRequest) request, opts);
        } else if (request instanceof WriteUserEventsRequest) {
            run = () -> doWriteUserEvents((WriteUserEventsRequest) request, opts);
        } else if (request instanceof AckServerImpressionsRequest) {
            run = () -> doAckImpression((AckServerImpressionsRequest) request, opts);
        } else {
            throw new RuntimeException("can't support this request type");
        }
        executor.execute(run);
    }

    private void doWriteUsers(WriteUsersRequest request, Option[] opts) {
        try {
            WriteUsersResponse response =
                    requestHelper.doWithRetry(client::writeUsers, request, opts, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncWriteUsers] success");
                return;
            }
            log.error("[AsyncWriteUsers] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncWriteUsers] occur error, msg:{}", e.getMessage());
        }
    }

    private void doWriteContents(WriteContentsRequest request, Option[] opts) {
        try {
            WriteContentsResponse response =
                    requestHelper.doWithRetry(client::writeContents, request, opts, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncWriteContents] success");
                return;
            }
            log.error("[AsyncWriteContents] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncWriteContents] occur error, msg:{}", e.getMessage());
        }
    }

    private void doWriteUserEvents(WriteUserEventsRequest request, Option[] opts) {
        try {
            WriteUserEventsResponse response =
                    requestHelper.doWithRetry(client::writeUserEvents, request, opts, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncWriteUserEvents] success");
                return;
            }
            log.error("[AsyncWriteUserEvents] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncWriteUserEvents] occur error, msg:{}", e.getMessage());
        }
    }

    private void doAckImpression(AckServerImpressionsRequest request, Option[] opts) {
        try {
            AckServerImpressionsResponse response =
                    requestHelper.doWithRetry(client::ackServerImpressions, request, opts, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncAckImpression] success");
                return;
            }
            log.error("[AsyncAckImpression] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncAckImpression] occur error, msg:{}", e.getMessage());
        }
    }
}
