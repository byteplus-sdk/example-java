package byteplus.example.general;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.RequestHelper.Callable;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.common.protocol.ByteplusCommon.OperationResponse;
import byteplus.sdk.common.protocol.ByteplusCommon.DoneResponse;
import byteplus.sdk.core.Option;
import byteplus.sdk.general.GeneralClient;
import lombok.extern.slf4j.Slf4j;
import com.google.protobuf.Parser;

import byteplus.sdk.general.protocol.ByteplusGeneral.ImportResponse;
import byteplus.sdk.general.protocol.ByteplusGeneral.WriteResponse;
import byteplus.sdk.general.protocol.ByteplusGeneral.CallbackRequest;
import byteplus.sdk.general.protocol.ByteplusGeneral.CallbackResponse;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
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

    private final GeneralClient client;

    private final RequestHelper requestHelper;

    public ConcurrentHelper(GeneralClient client) {
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
    public void submitWriteRequest(List<Map<String, Object>> dataList, String topic, Option... opts) {
        executor.submit(() -> doWrite(dataList, topic, opts));
    }

    public void submitDoneRequest(List<LocalDate> dateList, String topic, Option... opts) {
        executor.submit(() -> doDone(dateList, topic, opts));
    }

    public void submitCallbackRequest(CallbackRequest request, Option... opts) {
        executor.submit(() -> doCallback(request, opts));
    }


    private void doWrite(List<Map<String, Object>> dataList, String topic, Option... opts) {
        WriteResponse response;
        Callable<WriteResponse, List<Map<String, Object>>> call
                = (req, optList) -> client.writeData(req, topic, optList);
        try {
            response = requestHelper.doWithRetry(call, dataList, opts, RETRY_TIMES);
        } catch (Throwable e) {
            log.error("[AsyncWrite] occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("[AsyncWrite] success");
            return;
        }
        log.error("[AsyncWrite] find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    private void doDone(List<LocalDate> dateList, String topic, Option... opts) {
        DoneResponse response;
        Callable<DoneResponse, List<LocalDate>> call
                = (req, optList) -> client.done(req, topic, optList);
        try {
            response = requestHelper.doWithRetry(call, dateList, opts, RETRY_TIMES);
        } catch (Throwable e) {
            log.error("[AsyncDone] occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("[AsyncDone] success");
            return;
        }
        log.error("[AsyncDone] fail, rsp:{}", response);
    }

    private void doCallback(CallbackRequest request, Option... opts) {
        CallbackResponse response;
        try {
            response = requestHelper.doWithRetry(client::callback, request, opts, RETRY_TIMES);
        } catch (Throwable e) {
            log.error("[AsyncCallback] occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getCode())) {
            log.info("[AsyncCallback] success");
            return;
        }
        log.error("[AsyncCallback] fail, rsp:\n{}", response);
    }
}

