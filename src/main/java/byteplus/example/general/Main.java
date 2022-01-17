package byteplus.example.general;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.RequestHelper.Callable;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.byteair.protocol.ByteplusByteair;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.general.GeneralClient;
import byteplus.sdk.general.GeneralClientBuilder;
import byteplus.sdk.general.protocol.ByteplusGeneral;
import byteplus.sdk.general.protocol.ByteplusGeneral.PredictRequest;
import byteplus.sdk.general.protocol.ByteplusGeneral.PredictResponse;
import byteplus.sdk.general.protocol.ByteplusGeneral.WriteResponse;
import com.alibaba.fastjson.JSON;
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
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictCandidateItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictContext;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictExtra;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictRelatedItem;
import static byteplus.sdk.general.protocol.ByteplusGeneral.PredictUser;
import static byteplus.sdk.general.protocol.ByteplusGeneral.SearchCondition;

@Slf4j
public class Main {
    private final static GeneralClient client;

    private final static RequestHelper requestHelper;

    private final static int DEFAULT_RETRY_TIMES = 2;

    private final static Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(800);

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
                .build();
        requestHelper = new RequestHelper(client);
    }

    /**
     * Those examples request server with account named 'retail_demo',
     * The data in the "demo" account is only used for testing
     * and communication between customers.
     * Please don't send your private data by "demo" account.
     */
    public static void main(String[] args) {
        // upload data
        writeDataExample();

        // Mark some day's data has been entirely imported
        doneExample();

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
        // is better to less than 10000 when upload data.
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        Option[] opts = writeOptions();
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user";
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

    private static Option[] writeOptions() {
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(DEFAULT_WRITE_TIMEOUT),
                // The date of uploaded data
                // Incremental data uploading: required.
                // Historical data and real-time data uploading: not required.
                Option.withDataDate(LocalDate.of(2021, 8, 27))
                // The server is expected to return within a certain period，
                // to prevent can't return before client is timeout
                // Option.withServerTimeout(DEFAULT_WRITE_TIMEOUT.minus(Duration.ofMillis(100)))
        };
    }

    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 6, 10);
        List<LocalDate> dateList = Collections.singletonList(date);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = "user";
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
        // callbackExample(scene, predictRequest, predictResponse);
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

    // Report the recommendation request result (actual exposure data) through the callback interface
    public static void callbackExample(String scene, ByteplusGeneral.PredictRequest predictRequest,
                                       ByteplusGeneral.PredictResponse predictResponse) {
        List<ByteplusGeneral.CallbackItem> callbackItems = doSomethingWithPredictResult(predictResponse.getValue());
        ByteplusGeneral.CallbackRequest callbackRequest = ByteplusGeneral.CallbackRequest.newBuilder()
                .setPredictRequestId(predictResponse.getRequestId())
                // required, should be consistent with the uid passed in the recommendation request
                .setUid(predictRequest.getUser().getUid())
                // required，should be consistent with `scene` used in the recommendation request
                .setScene(scene)
                .addAllItems(callbackItems)
                .build();

        Option[] opts = defaultOptions(DEFAULT_CALLBACK_TIMEOUT);
        ByteplusGeneral.CallbackResponse callbackResponse = null;
        try {
            callbackResponse = client.callback(callbackRequest, opts);
        } catch (NetException | BizException e) {
            e.printStackTrace();
        } finally {
            log.info("callback rsp info: {} \n", callbackResponse);
        }
    }

    private static List<ByteplusGeneral.CallbackItem> doSomethingWithPredictResult(ByteplusGeneral.PredictResult predictResult) {
        // You can handle recommend results here,
        // such as filter, insert other items, sort again, etc.
        // The list of goods finally displayed to user and the filtered goods
        // should be sent back to bytedance for deduplication
        return conv2CallbackItems(predictResult.getItemsList());
    }

    private static List<ByteplusGeneral.CallbackItem> conv2CallbackItems(List<ByteplusGeneral.PredictResultItem> resultItems) {
        if (Objects.isNull(resultItems) || resultItems.isEmpty()) {
            return Collections.emptyList();
        }
        List<ByteplusGeneral.CallbackItem> callbackItems = new ArrayList<>(resultItems.size());
        for (int i = 0; i < resultItems.size(); i++) {
            ByteplusGeneral.PredictResultItem resultItem = resultItems.get(i);
            Map<String, String> extraMap = Collections.singletonMap("reason", "kept");
            ByteplusGeneral.CallbackItem callbackItem = ByteplusGeneral.CallbackItem.newBuilder()
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
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(timeout),
        };
    }
}
