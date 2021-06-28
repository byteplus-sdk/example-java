package byteplus.example.retail;

import byteplus.retail.sdk.protocol.ByteplusRetail.AckServerImpressionsRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.AckServerImpressionsResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportProductsRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportProductsResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportUserEventsRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportUserEventsResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportUsersRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.ImportUsersResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteProductsRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteProductsResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteUserEventsRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteUserEventsResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteUsersRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.WriteUsersResponse;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.Option;
import byteplus.sdk.retail.RetailClient;
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
    public void submitRequest(Object request, Option... opts) throws BizException {
        Handler handler;
        if (request instanceof WriteUsersRequest) {
            handler = new Handler(TaskType.WRITE_USERS, request, opts);
        } else if (request instanceof ImportUsersRequest) {
            handler = new Handler(TaskType.IMPORT_USERS, request, opts);
        } else if (request instanceof WriteProductsRequest) {
            handler = new Handler(TaskType.WRITE_PRODUCTS, request, opts);
        } else if (request instanceof ImportProductsRequest) {
            handler = new Handler(TaskType.IMPORT_PRODUCTS, request, opts);
        } else if (request instanceof WriteUserEventsRequest) {
            handler = new Handler(TaskType.WRITE_USER_EVENTS, request, opts);
        } else if (request instanceof ImportUserEventsRequest) {
            handler = new Handler(TaskType.IMPORT_USER_EVENTS, request, opts);
        } else if (request instanceof AckServerImpressionsRequest) {
            handler = new Handler(TaskType.ACK_IMPRESSION, request, opts);
        } else {
            throw new BizException("can't support this request type");
        }
        executor.execute(handler);
    }

    enum TaskType {
        WRITE_USERS,
        IMPORT_USERS,
        WRITE_PRODUCTS,
        IMPORT_PRODUCTS,
        WRITE_USER_EVENTS,
        IMPORT_USER_EVENTS,
        ACK_IMPRESSION
    }

    private class Handler implements Runnable {
        private final TaskType type;
        private final Object request;
        private final Option[] opts;

        private Handler(TaskType type, Object request, Option... opts) {
            this.type = type;
            this.request = request;
            this.opts = opts;
        }

        @Override
        public void run() {
            switch (type) {
                case WRITE_USERS:
                    doWriteUsers((WriteUsersRequest) request);
                    break;
                case IMPORT_USERS:
                    doImportUsers((ImportUsersRequest) request);
                    break;
                case WRITE_PRODUCTS:
                    doWriteProducts((WriteProductsRequest) request);
                    break;
                case IMPORT_PRODUCTS:
                    doImportProducts((ImportProductsRequest) request);
                    break;
                case WRITE_USER_EVENTS:
                    doWriteUserEvents((WriteUserEventsRequest) request);
                    break;
                case IMPORT_USER_EVENTS:
                    doImportUserEvents((ImportUserEventsRequest) request);
                    break;
                case ACK_IMPRESSION:
                    doAckImpression((AckServerImpressionsRequest) request);
                    break;
            }
        }

        private void doWriteUsers(WriteUsersRequest request) {
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

        private void doImportUsers(ImportUsersRequest request) {
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

        private void doWriteProducts(WriteProductsRequest request) {
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

        private void doImportProducts(ImportProductsRequest request) {
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

        private void doWriteUserEvents(WriteUserEventsRequest request) {
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

        private void doImportUserEvents(ImportUserEventsRequest request) {
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

        private void doAckImpression(AckServerImpressionsRequest request) {
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
}

