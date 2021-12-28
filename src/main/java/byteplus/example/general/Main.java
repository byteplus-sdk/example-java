package byteplus.example.general;

import byteplus.example.common.Example;
import byteplus.example.common.RequestHelper;
import byteplus.example.common.RequestHelper.Callable;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.common.protocol.ByteplusCommon.Operation;
import byteplus.sdk.common.protocol.ByteplusCommon.OperationResponse;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.general.GeneralClient;
import byteplus.sdk.general.GeneralClientBuilder;
import byteplus.sdk.general.protocol.ByteplusGeneral.ImportResponse;
import byteplus.sdk.general.protocol.ByteplusGeneral.PredictRequest;
import byteplus.sdk.general.protocol.ByteplusGeneral.PredictResponse;
import byteplus.sdk.general.protocol.ByteplusGeneral.PredictResult;
import byteplus.sdk.general.protocol.ByteplusGeneral.WriteResponse;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static byteplus.sdk.common.protocol.ByteplusCommon.DoneResponse;
import static byteplus.sdk.general.protocol.ByteplusGeneral.CallbackItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.CallbackRequest;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictCandidateItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictContext;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictExtra;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictRelatedItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictResultItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictUser;
import static byteplus.sdk.general.protocol.ByteplusGeneral.SearchCondition;

@Slf4j
public class Main {
    private final static GeneralClient client;

    private final static RequestHelper requestHelper;

    private final static ConcurrentHelper concurrentHelper;

    private final static int DEFAULT_RETRY_TIMES = 2;

    private final static Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_IMPORT_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_DONE_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_PREDICT_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_CALLBACK_TIMEOUT = Duration.ofMillis(800);

    // A unique token assigned by bytedance, which is used to
    // generate an authenticated signature when building a request.
    // It is sometimes called "secret".
    public final static String TOKEN = "xxxxxxxxxxxxxxxxxxxxx";

    // A unique ID assigned by Bytedance, which is used to
    // generate an authenticated signature when building a request
    // It is sometimes called "appkey".
    public final static String TENANT_ID = "xxxxxxxxxxxx";

    // A unique identity assigned by Bytedance, which is need to fill in URL.
    // It is sometimes called "company".
    public final static String TENANT = "general_demo";

    static {
        client = new GeneralClientBuilder()
                .tenant(TENANT) // Required
                .tenantId(TENANT_ID) // Required
                .token(TOKEN) // Required
                .region(Region.CN) //Required
//                .schema("https") //Optional
//                .headers(Collections.singletonMap("Customer-Header", "value")) // Optional
                .build();
        requestHelper = new RequestHelper(client);
        concurrentHelper = new ConcurrentHelper(client);
    }

    /**
     * Those examples request server with account named 'retail_demo',
     * The data in the "demo" account is only used for testing
     * and communication between customers.
     * Please don't send your private data by "demo" account.
     */
    public static void main(String[] args) {
        // Write real-time user data
        writeDataExample();
        // Write real-time data concurrently
        concurrentWriteDataExample();

        // Import daily offline data
        importDataExample();
        // Import daily offline data concurrently
        concurrentImportDataExample();

        // Mark some day's data has been entirely imported
        doneExample();
        // Do 'done' request concurrently
        concurrentDoneExample();

        // Obtain Operation information according to operationName,
        // if the corresponding task is executing, the real-time
        // result of task execution will be returned
        getOperationExample();

        // Lists operations that match the specified filter in the request.
        // It can be used to retrieve the task when losing 'operation.name',
        // or to statistic the execution of the task within the specified range,
        // for example, the total count of successfully imported data.
        // The result of "listOperations" is not real-time.
        // The real-time info should be obtained through "getOperation"
        listOperationsExample();

        // Get recommendation results
        recommendExample();

        // Do search request
        searchExample();

        try {
            // Pause for 5 seconds until the asynchronous import task completes
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {

        }
        client.release();
        System.exit(0);
    }

    public static void writeDataExample() {
        // The count of items included in one "Write" request
        // is better to less than 100 when upload real-time data.
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        Option[] opts = writeOptions();
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        WriteResponse response;
        try {
            Callable<WriteResponse, List<Map<String, Object>>> call
                    = (req, optList) -> client.writeData(req, topic, optList);
            response = requestHelper.doWithRetry(call, dataList, opts, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("write data occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write data success");
            return;
        }
        log.error("write data find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    public static void concurrentWriteDataExample() {
        // The count of items included in one "Write" request
        // is better to less than 100 when upload real-time data.
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        Option[] opts = writeOptions();
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        concurrentHelper.submitWriteRequest(dataList, topic, opts);
    }

    private static Option[] writeOptions() {
        // All options are optional
        Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(DEFAULT_WRITE_TIMEOUT),
                Option.withHeaders(customerHeaders),
                // The server is expected to return within a certain period，
                // to prevent can't return before client is timeout
                Option.withServerTimeout(DEFAULT_WRITE_TIMEOUT.minus(Duration.ofMillis(100)))
        };
    }

    public static void importDataExample() {
        // The count of items included in one "Import" request is max to 10k.
        // The server will reject request if items are too many.
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        Option[] opts = importOptions();
        ImportResponse response;
        Parser<ImportResponse> rspParser = ImportResponse.parser();
        Callable<OperationResponse, List<Map<String, Object>>> call
                = (req, optList) -> client.importData(req, topic, optList);
        try {
            response = requestHelper.doImport(call, dataList, opts, rspParser, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("import data occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("import data success");
            return;
        }
        log.error("import data find failure info, msg:{} errSamples:{}",
                response.getStatus(), response.getErrorSamplesList());
    }

    public static void concurrentImportDataExample() {
        // The count of items included in one "Import" request is max to 10k.
        // The server will reject request if items are too many.
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        Option[] opts = importOptions();
        concurrentHelper.submitImportRequest(dataList, topic, opts);
    }

    private static Option[] importOptions() {
        // All options are optional
        Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(DEFAULT_IMPORT_TIMEOUT),
                Option.withHeaders(customerHeaders),
                // Required for import request
                // The date in produced of data in this 'import' request
                Option.withDataDate(LocalDate.now())
                // If data in a whole day has been imported completely,
                // the import request need be with this option
                // Option.withDataEnd(true)
        };
    }

    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 6, 10);
        List<LocalDate> dateList = Collections.singletonList(date);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        Option[] opts = defaultOptions(DEFAULT_DONE_TIMEOUT);
        Callable<DoneResponse, List<LocalDate>> call
                = (req, optList) -> client.done(req, topic, optList);
        DoneResponse response;
        try {
            response = requestHelper.doWithRetry(call, dateList, opts, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("[Done] occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("[Done] success");
            return;
        }
        log.error("[Done] find failure info, rsp:{}", response);
    }

    private static void concurrentDoneExample() {
        LocalDate date = LocalDate.of(2021, 6, 10);
        List<LocalDate> dateList = Collections.singletonList(date);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user_event";
        Option[] opts = defaultOptions(DEFAULT_DONE_TIMEOUT);
        concurrentHelper.submitDoneRequest(dateList, topic, opts);
    }

    public static void getOperationExample() {
        String name = "0c5a1145-2c12-4b83-8998-2ae8153ca089";
        Example.getOperationExample(client, name);
    }

    public static void listOperationsExample() {
        String filter = "date>=2021-06-15 and done=true";
        List<Operation> operations = Example.listOperationsExample(client, filter);
        parseTaskResponse(operations);
    }

    private static void parseTaskResponse(List<Operation> operations) {
        if (Objects.isNull(operations) || operations.isEmpty()) {
            return;
        }
        for (Operation operation : operations) {
            Any responseAny = operation.getResponse();
            String typeUrl = responseAny.getTypeUrl();
            // To ensure compatibility, do not parse response by 'Any.unpack()'
            try {
                if (typeUrl.contains("ImportResponse")) {
                    ImportResponse importResponse = ImportResponse.parseFrom(responseAny.getValue());
                    log.info("[ListOperations] import rsp:\n{}", importResponse);
                } else {
                    log.error("[ListOperations] unexpected task response type:{}", typeUrl);
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("[ListOperations] parse task response fail, msg:{}", e.getMessage());
            }
        }
    }

    public static void recommendExample() {
        PredictRequest predictRequest = buildPredictRequest();
        Option[] predictOpts = defaultOptions(DEFAULT_PREDICT_TIMEOUT);
        PredictResponse predictResponse;
        // The `scene` is provided by ByteDance, according to tenant's situation
        String scene = "home";
        try {
            predictResponse = client.predict(predictRequest, scene, predictOpts);
        } catch (Exception e) {
            log.error("predict occur error, msg:{}", e.getMessage());
            return;
        }
        if (!StatusHelper.isSuccess(predictResponse.getCode())) {
            log.error("predict find failure info, msg:{}", predictResponse);
            return;
        }
        log.info("predict success");
        // The items, which is eventually shown to user,
        // should send back to Bytedance for deduplication
        List<CallbackItem> callbackItems = doSomethingWithPredictResult(predictResponse.getValue());
        CallbackRequest callbackRequest = CallbackRequest.newBuilder()
                .setPredictRequestId(predictResponse.getRequestId())
                .setUid(predictRequest.getUser().getUid())
                .setScene(scene)
                .addAllItems(callbackItems)
                .build();
        Option[] ackOpts = defaultOptions(DEFAULT_CALLBACK_TIMEOUT);
        concurrentHelper.submitCallbackRequest(callbackRequest, ackOpts);
    }

    private static PredictRequest buildPredictRequest() {
        PredictUser user = PredictUser.newBuilder()
                .setUid("uid")
                .build();
        PredictContext context = PredictContext.newBuilder()
                .setSpm("xx$$xxx$$xx")
                .build();
        PredictCandidateItem candidateItem = PredictCandidateItem.newBuilder()
                .setId("item_id")
                .build();
        PredictRelatedItem relatedItem = PredictRelatedItem.newBuilder()
                .setId("item_id")
                .build();

        PredictExtra extra = PredictExtra.newBuilder()
                .putExtra("extra_key", "value")
                .build();

        return PredictRequest.newBuilder()
                .setUser(user)
                .setContext(context)
                .setSize(20)
                .addCandidateItems(candidateItem)
                .setRelatedItem(relatedItem)
                .setExtra(extra)
                .build();
    }

    private static List<CallbackItem> doSomethingWithPredictResult(PredictResult predictResult) {
        // You can handle recommend results here,
        // such as filter, insert other items, sort again, etc.
        // The list of goods finally displayed to user and the filtered goods
        // should be sent back to bytedance for deduplication
        return conv2CallbackItems(predictResult.getItemsList());
    }

    private static List<CallbackItem> conv2CallbackItems(List<PredictResultItem> resultItems) {
        if (Objects.isNull(resultItems) || resultItems.isEmpty()) {
            return Collections.emptyList();
        }
        List<CallbackItem> callbackItems = new ArrayList<>(resultItems.size());
        for (int i = 0; i < resultItems.size(); i++) {
            PredictResultItem resultItem = resultItems.get(i);
            Map<String, String> extraMap = Collections.singletonMap("reason", "kept");
            CallbackItem callbackItem = CallbackItem.newBuilder()
                    .setId(resultItem.getId())
                    .setPos((i + 1) + "")
                    .setExtra(JSON.toJSONString(extraMap))
                    .build();
            callbackItems.add(callbackItem);
        }
        return callbackItems;
    }

    public static void searchExample() {
        PredictRequest searchRequest = buildSearchRequest();
        Option[] opts = defaultOptions(DEFAULT_PREDICT_TIMEOUT);
        PredictResponse searchResponse;
        // The `scene` is provided by ByteDance,
        // that usually is "search" in search request
        String scene = "search";
        try {
            searchResponse = client.predict(searchRequest, scene, opts);
        } catch (Exception e) {
            log.error("search occur error, msg:{}", e.getMessage());
            return;
        }
        if (!StatusHelper.isSuccess(searchResponse.getCode())) {
            log.error("search find failure info, msg:{}", searchResponse);
            return;
        }
        log.info("search success");
    }

    private static PredictRequest buildSearchRequest() {
        SearchCondition condition = SearchCondition.newBuilder()
                .setSearchType(0)
                .setQuery("adidas")
                .build();

        PredictExtra extra = PredictExtra.newBuilder()
                .putExtra("extra_key", "value")
                .build();

        return PredictRequest.newBuilder()
                .setSize(20)
                .setSearchCondition(condition)
                .setExtra(extra)
                .build();
    }

    private static Option[] defaultOptions(Duration timeout) {
        // All options are optional
//        Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(timeout),
//                Option.withHeaders(customerHeaders)
        };
    }
}
