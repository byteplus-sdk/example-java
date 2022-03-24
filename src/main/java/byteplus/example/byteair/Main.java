package byteplus.example.byteair;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.RequestHelper.Callable;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.byteair.ByteairClient;
import byteplus.sdk.byteair.ByteairClientBuilder;
import byteplus.sdk.byteair.protocol.ByteplusByteair;
import byteplus.sdk.common.protocol.ByteplusCommon.DoneResponse;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.byteair.protocol.ByteplusByteair.WriteResponse;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictRequest;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictUser;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictContext;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictCandidateItem;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictParentItem;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictResult;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictItem;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictExtra;
import byteplus.sdk.byteair.protocol.ByteplusByteair.PredictResponse;
import byteplus.sdk.byteair.protocol.ByteplusByteair.CallbackRequest;
import byteplus.sdk.byteair.protocol.ByteplusByteair.CallbackItem;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

@Slf4j
public class Main {
    private final static ByteairClient client;

    private final static RequestHelper requestHelper;

    private final static ConcurrentHelper concurrentHelper;

    private final static int DEFAULT_RETRY_TIMES = 2;

    private final static Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(1000);

    private final static Duration DEFAULT_DONE_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_PREDICT_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_CALLBACK_TIMEOUT = Duration.ofMillis(800);

    /**
     * 租户相关信息
     */
    //火山引擎账号/子账号的AccessKey
    public final static String AK = "xxxxxxxx";

    //火山引擎账号/子账号的SecretKey
    public final static String SK = "xxxxxxxx";

    //火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
    public final static String TENANT_ID = "xxxxxxxx";

    //个性化推荐服务新建的项目id(project_id)，如"1231314"
    public final static String PROJECT_ID = "xxxxxxxx";

    /**
     * stage枚举值，与推荐平台四种同步阶段相对应
     */
    // 增量实时数据同步阶段
    public final static String STAGE_INCREMENTAL_SYNC_STREAMING = "incremental_sync_streaming";

    // 增量天级数据同步阶段
    public final static String STAGE_INCREMENTAL_SYNC_DAILY = "incremental_sync_daily";

    // 测试数据/预同步阶段
    public final static String STAGE_PRE_SYNC = "pre_sync";

    // 历史数据同步阶段
    public final static String STAGE_HISTORY_SYNC = "history_sync";

    /**
     * 标准数据topic枚举值，包括：item(物品，如商品、媒资数据、社区内容等)、user(用户)、behavior(行为)
     */
    // 物品
    public final static String TOPIC_ITEM = "item";

    // 用户
    public final static String TOPIC_USER = "user";

    // 行为
    public final static String TOPIC_BEHAVIOR = "behavior";

    // DefaultPredictScene 产品化目前的predict scene可不填，默认为"default"
    public final static String DEFAULT_PREDICT_SCENE = "default";

    static {
        client = new ByteairClientBuilder()
                .projectId(PROJECT_ID) // 必传，项目id
                .tenantId(TENANT_ID) // 必传
                .region(Region.AIR_CN) //必传，推荐平台国内版Region.AIR_CN，海外版填Region.AIR_SG
                .ak(AK)
                .sk(SK)
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

        // 标识天级离线数据上传完成
        doneExample();

        // 请求推荐服务获取推荐结果
        recommendExample();

        try {
            // 等待异步任务完成
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
        String topic = TOPIC_USER;
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

    // Write请求参数说明，请根据说明修改
    private static Option[] writeOptions() {
        return new Option[]{
                // 必选. Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
                Option.withStage(STAGE_INCREMENTAL_SYNC_STREAMING),
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 离线数据同步时【必传】，数据产生日期，实际传输时需修改为实际日期
                // 实时数据同步时【不传】此Option
                Option.withDataDate(LocalDate.of(2021, 11, 1)),
                // 可选，请求超时时间
                Option.withTimeout(DEFAULT_WRITE_TIMEOUT),
                // 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
                // 此服务器超时应小于Write请求设置的总超时。
                // Option.withServerTimeout(DEFAULT_WRITE_TIMEOUT.minus(Duration.ofMillis(100))),
        };
    }


    // 离线天级数据上传完成后Done接口example
    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 8, 1);
        // 已经上传完成的数据日期，可在一次请求中传多个
        List<LocalDate> dateList = Collections.singletonList(date);
        // 与离线天级数据传输的topic保持一致
        String topic = TOPIC_USER;
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

    // Done请求参数说明，请根据说明修改
    private static Option[] doneOptions() {
        return new Option[]{
                // 必选，与Import接口数据传输阶段保持一致，包括：
                // 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
                Option.withStage(STAGE_PRE_SYNC),
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 可选，请求超时时间
                Option.withTimeout(DEFAULT_DONE_TIMEOUT),
        };
    }

    // 推荐服务请求example
    public static void recommendExample() {
        PredictRequest predictRequest = buildPredictRequest();
        Option[] predictOpts = predictOptions(DEFAULT_PREDICT_TIMEOUT);
        PredictResponse predictResponse;
        try {
            predictResponse = client.predict(predictRequest, predictOpts);
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
                .setScene(DEFAULT_PREDICT_SCENE)
                .addAllItems(callbackItems)
                .build();
        Option[] callbackOptions = defaultOptions(DEFAULT_CALLBACK_TIMEOUT);
        concurrentHelper.submitCallbackRequest(callbackRequest, callbackOptions);
    }

    // 推荐请求options
    private static Option[] predictOptions(Duration timeout) {
        // All options are optional
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(timeout),
                // 推荐场景，目前统一填default或者不填
                //Option.withScene("default")
        };
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
        PredictParentItem parentItem = PredictParentItem.newBuilder()
                .setId("item_id")
                .build();

        PredictExtra extra = PredictExtra.newBuilder()
                .putExtra("extra_key", "value")
                .build();

        return PredictRequest.newBuilder()
                .setUser(user)
                .setContext(context)
                .addCandidateItems(candidateItem)
                .setParentItem(parentItem)
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

    private static List<CallbackItem> conv2CallbackItems(List<PredictItem> resultItems) {
        if (Objects.isNull(resultItems) || resultItems.isEmpty()) {
            return Collections.emptyList();
        }
        List<CallbackItem> callbackItems = new ArrayList<>(resultItems.size());
        for (int i = 0; i < resultItems.size(); i++) {
            PredictItem resultItem = resultItems.get(i);
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
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(timeout),
        };
    }
}
