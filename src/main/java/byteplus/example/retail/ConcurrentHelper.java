package byteplus.example.retail;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.core.Option;
import byteplus.sdk.retail.RetailClient;
import byteplus.sdk.retail.protocol.ByteplusRetail.AckServerImpressionsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.AckServerImpressionsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportProductsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportProductsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUserEventsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUserEventsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUsersRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUsersResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteProductsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteProductsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUserEventsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUserEventsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUsersRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUsersResponse;
import com.google.protobuf.Parser;
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

    private final RetailClient client;

    private final RequestHelper requestHelper;

    public ConcurrentHelper(RetailClient client) {
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
        } else if (request instanceof ImportUsersRequest) {
            run = () -> doImportUsers((ImportUsersRequest) request, opts);
        } else if (request instanceof WriteProductsRequest) {
            run = () -> doWriteProducts((WriteProductsRequest) request, opts);
        } else if (request instanceof ImportProductsRequest) {
            run = () -> doImportProducts((ImportProductsRequest) request, opts);
        } else if (request instanceof WriteUserEventsRequest) {
            run = () -> doWriteUserEvents((WriteUserEventsRequest) request, opts);
        } else if (request instanceof ImportUserEventsRequest) {
            run = () -> doImportUserEvents((ImportUserEventsRequest) request, opts);
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

    private void doImportUsers(ImportUsersRequest request, Option[] opts) {
        try {
            Parser<ImportUsersResponse> parser = ImportUsersResponse.parser();
            ImportUsersResponse response =
                    requestHelper.doImport(client::importUsers, request, opts, parser, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncImportUsers] success");
                return;
            }
            log.error("[AsyncImportUsers] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncImportUsers] occur error, msg:{}", e.getMessage());
        }
    }

    private void doWriteProducts(WriteProductsRequest request, Option[] opts) {
        try {
            WriteProductsResponse response =
                    requestHelper.doWithRetry(client::writeProducts, request, opts, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncWriteProducts] success");
                return;
            }
            log.error("[AsyncWriteProducts] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncWriteProducts] occur error, msg:{}", e.getMessage());
        }
    }

    private void doImportProducts(ImportProductsRequest request, Option[] opts) {
        try {
            Parser<ImportProductsResponse> parser = ImportProductsResponse.parser();
            ImportProductsResponse response =
                    requestHelper.doImport(client::importProducts, request, opts, parser, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncImportProducts] success");
                return;
            }
            log.error("[AsyncImportProducts] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncImportProducts] occur error, msg:{}", e.getMessage());
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

    private void doImportUserEvents(ImportUserEventsRequest request, Option[] opts) {
        try {
            Parser<ImportUserEventsResponse> parser = ImportUserEventsResponse.parser();
            ImportUserEventsResponse response =
                    requestHelper.doImport(client::importUserEvents, request, opts, parser, RETRY_TIMES);
            if (StatusHelper.isSuccess(response.getStatus())) {
                log.info("[AsyncImportUserEvents] success");
                return;
            }
            log.error("[AsyncImportUserEvents] fail, rsp:\n{}", response);
        } catch (Throwable e) {
            log.error("[AsyncImportUserEvents] occur error, msg:{}", e.getMessage());
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

