package byteplus.example.byteair;

import byteplus.example.common.Example;
import byteplus.example.common.RequestHelper;
import byteplus.example.common.RequestHelper.Callable;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.common.protocol.ByteplusCommon.OperationResponse;
import byteplus.sdk.common.protocol.ByteplusCommon.DoneResponse;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.general.GeneralClient;
import byteplus.sdk.general.GeneralClientBuilder;
import byteplus.sdk.general.protocol.ByteplusGeneral.*;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

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

    static {
        client = new GeneralClientBuilder()
                .tenant(Constant.PROJECT_ID) // 必传，项目id
                .tenantId(Constant.TENANT_ID) // 必传
                .token(Constant.TOKEN) // 必传
                .region(Region.AIR) //必传，必须填Region.AIR，默认使用byteair-api-cn1.snssdk.com为host
                .hosts(Collections.singletonList("byteair-api-cn1.snssdk.com"))//可选，如果设置了region则host可不设置
                .schema("https") //可选，仅支持"https"和"http"
//              .headers(Collections.singletonMap("Customer-Header", "value")) // 可选，添加自定义header
                .build();
        requestHelper = new RequestHelper(client);
        concurrentHelper = new ConcurrentHelper(client); //用于多线程请求
    }

    /**
     * 下面example请求中使用的是demo的参数，可能无法直接请求通过，
     * 需要替换Constant.java中相关参数为真实参数
     */
    public static void main(String[] args) {
        // 实时数据上传
        writeDataExample();
        // 并发实时数据上传
        concurrentWriteDataExample();

        // 天级离线数据上传
        importDataExample();
        // 并发天级离线数据上传
        concurrentImportDataExample();

        // 标识天级离线数据上传完成
        doneExample();
        // 并发标识天级离线数据上传完成
        concurrentDoneExample();

        // 与Import接口一起使用，用于天级数据上传状态（是否处理完成，成功/失败条数）监听
        getOperationExample();

        // 请求推荐服务获取推荐结果
        recommendExample();

        try {
            // 等待异步任务Import完成
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {

        }
        client.release();
        System.exit(0);
    }


    // 增量实时数据上传example
    public static void writeDataExample() {
        // 此处为测试数据，实际调用时需注意字段类型和格式，实际请求时每次请求不超过300条数据
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        Option[] opts = writeOptions();
        // topic为枚举值，请参考API文档
        String topic = Constant.TOPIC_USER;
        WriteResponse response;
        try {
            Callable<WriteResponse, List<Map<String, Object>>> call
                    = (req, optList) -> client.writeData(req, topic, optList);
            // 带重试的请求，自行实现重试时请参考此处重试逻辑
            response = requestHelper.doWithRetry(call, dataList, opts, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("write data occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write data success");
            return;
        }
        // 出现错误、异常时请记录好日志，方便自行排查问题
        log.error("write data find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    // 增量实时数据并发/异步上传example
    public static void concurrentWriteDataExample() {
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        Option[] opts = writeOptions();
        String topic = Constant.TOPIC_USER;
        concurrentHelper.submitWriteRequest(dataList, topic, opts);
    }

    // Write请求参数说明，请根据说明修改
    private static Option[] writeOptions() {
        // Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                // 必选. Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
                Option.withStage(Constant.STAGE_INCREMENTAL_SYNC_STREAMING),
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 可选，请求超时时间
                Option.withTimeout(DEFAULT_WRITE_TIMEOUT),
                // 可选. 添加自定义header.
                // Option.withHeaders(customerHeaders),
                // 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
                // 此服务器超时应小于Write请求设置的总超时。
                Option.withServerTimeout(DEFAULT_WRITE_TIMEOUT.minus(Duration.ofMillis(100))),
        };
    }

    // 离线天级数据上传example
    public static void importDataExample() {
        // 一个“Import”请求中包含的数据条数最多为10k，如果数据太多，服务器将拒绝请求。
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        String topic = Constant.TOPIC_USER;
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

    // 离线天级数据并发/异步上传example
    public static void concurrentImportDataExample() {
        List<Map<String, Object>> dataList = MockHelper.mockDataList(2);
        String topic = Constant.TOPIC_USER;
        Option[] opts = importOptions();
        concurrentHelper.submitImportRequest(dataList, topic, opts);
    }

    // Import请求参数说明，请根据说明修改
    private static Option[] importOptions() {
        // Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                // 必选， Import接口数据传输阶段，包括：
                // 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
                Option.withStage(Constant.STAGE_PRE_SYNC),
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 可选，请求超时时间
                Option.withTimeout(DEFAULT_IMPORT_TIMEOUT),
                // 必传，数据产生日期，实际传输时需修改为实际日期
                Option.withDataDate(LocalDate.of(2021, 8, 27)),
                // 可选. 添加自定义header.
                // Option.withHeaders(customerHeaders),
        };
    }

    // 离线天级数据上传完成后Done接口example
    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 8, 1);
        // 已经上传完成的数据日期，可在一次请求中传多个
        List<LocalDate> dateList = Collections.singletonList(date);
        // 与离线天级数据传输的topic保持一致
        String topic = Constant.TOPIC_USER;
        Option[] opts = doneOptions();
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

    // 离线天级数据上传完成后异步Done接口example，done接口一般无需异步
    private static void concurrentDoneExample() {
        LocalDate date = LocalDate.of(2021, 6, 10);
        List<LocalDate> dateList = Collections.singletonList(date);
        // The `topic` is some enums provided by bytedance,
        // who according to tenant's situation
        String topic = Constant.TOPIC_USER;
        Option[] opts = doneOptions();
        concurrentHelper.submitDoneRequest(dateList, topic, opts);
    }

    // Done请求参数说明，请根据说明修改
    private static Option[] doneOptions() {
        // Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                // 必选，与Import接口数据传输阶段保持一致，包括：
                // 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
                Option.withStage(Constant.STAGE_PRE_SYNC),
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 可选，请求超时时间
                Option.withTimeout(DEFAULT_DONE_TIMEOUT),
                // 可选，自定义header
                // Option.withHeaders(customerHeaders)
        };
    }

    // getOperation接口使用example，一般与Import接口一起使用，用于天级数据上传状态监听
    public static void getOperationExample() {
        String name = "0c5a1145-2c12-4b83-8998-2ae8153ca089";
        Example.getOperationExample(client, name);
    }

    // 推荐服务请求example
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

    private static Option[] defaultOptions(Duration timeout) {
        // All options are optional
        Map<String, String> customerHeaders = Collections.emptyMap();
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(timeout),
                Option.withHeaders(customerHeaders)
        };
    }
}
