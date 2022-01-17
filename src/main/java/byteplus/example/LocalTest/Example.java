package byteplus.example.LocalTest;

import byteplus.sdk.common.protocol.ByteplusCommon;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.general.GeneralClient;
import byteplus.sdk.general.GeneralClientBuilder;
import byteplus.sdk.general.protocol.ByteplusGeneral;
import com.alibaba.fastjson.JSON;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

public final class Example {
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
    private static GeneralClient client;

    public static void main(String[] args) {
        init();
        writeDataExample();
    }

    private static void init() {
        //创建client
        client = new GeneralClientBuilder()
                .hosts(Collections.singletonList("10.1.57.26:5879"))
                .tenant("20001459") // Required, 字节为客户分配的唯一标识名
                .tenantId("1") // Required，客户id，分配给客户的appKey，会随密钥发给客户
                .token("fb541d45237fdd44b00255ba2ea77e48") // Required，用于鉴权加密的密钥，每个客户独立分配
                .region(Region.CN) //Required，服务在中国则用Region.CN，新加坡为Region.SG,美东为Region.US
                .schema("http")
                .build();
    }

    public static void writeDataExample() {
        //构造数据
        Map<String, Object> item = new HashMap<>();
        // 以下数据皆为测试数据，实际调用时需注意字段类型和格式
        item.put("user_id", "xxxxxxx");
        item.put("register_time", 12345678);
        item.put("age", "20-30");
        item.put("gender", "female");

        List<Map<String, Object>> datas = new ArrayList<>();
        datas.add(item);
        Option[] opts = writeOptions();
        ByteplusGeneral.WriteResponse writeResponse = null;
        //同步执行上传
        try {
            writeResponse = client.writeData(datas, "user", opts);
        } catch (NetException | BizException e) {
            System.out.printf("upload occur error, msg:%s\n", e.getMessage());
            return;
        }
        if (writeResponse.getStatus().getCode() == 0) {
            System.out.printf("upload success, msg:%s\n", writeResponse);
            return;
        }
        System.out.printf("upload fail, msg:%s errItems:%s\n", writeResponse.getStatus(), writeResponse.getErrorsList());
    }

    private static Option[] writeOptions() {
        return new Option[]{
                // 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
                Option.withRequestId(UUID.randomUUID().toString()),
                // 强烈建议增量天级数据上传时带上，数据产生日期，实际传输时需修改为实际日期
                // 实时数据同步时不加此Option
                Option.withDataDate(LocalDate.of(2021, 9, 14)),
                // 可选，请求超时时间
                Option.withTimeout(Duration.ofMillis(800)),
                Option.withStage(STAGE_INCREMENTAL_SYNC_DAILY),

        };
    }

    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 8, 1); //替换成对应需要执行done的日期
        List<LocalDate> partitionDateList = Collections.singletonList(date);
        String topic = "user"; //替换成对应需要执行done的topic，
        Option[] opts = doneOptions();
        ByteplusCommon.DoneResponse doneResponse = null;
        try {
            doneResponse = client.done(partitionDateList, topic, opts);
        } catch (BizException | NetException e) {
            System.out.printf("done occur error, msg:%s \n", e.getMessage());
            return;
        }
        if (doneResponse.getStatus().getCode() == 0) {
            System.out.printf("done success, msg:%s\n", doneResponse);
            return;
        }
        System.out.printf("done fail, msg:%s \n", doneResponse.getStatus());
    }

    private static Option[] doneOptions() {
        return new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(Duration.ofMillis(800)),
        };
    }

    public void predictDemo() {
        ByteplusGeneral.PredictRequest predictRequest = buildPredictRequest();
        Option[] predictOpts = new Option[]{
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(Duration.ofMillis(800)),
        };
        ByteplusGeneral.PredictResponse predictResponse = null;
        // The `scene` is provided by ByteDance, according to tenant's situation
        String scene = "home";
        try {
            predictResponse = client.predict(predictRequest, scene, predictOpts);
        } catch (Exception e) {
            System.out.printf("predict occur error, msg:%s \n", e.getMessage());
            return;
        }
        if (Objects.nonNull(predictResponse) &&
                (predictResponse.getCode() == 0 || predictResponse.getCode() == 200)) {
            System.out.printf("predict success, rsp:%s \n", predictResponse);
            return;
        }
        System.out.printf("predict fail, rsp:%s \n", predictResponse);

    }


    private static ByteplusGeneral.PredictRequest buildPredictRequest() {
        ByteplusGeneral.PredictUser user = ByteplusGeneral.PredictUser.newBuilder()
                .setUid("uid")
                .build();
        ByteplusGeneral.PredictContext context = ByteplusGeneral.PredictContext.newBuilder()
                .setSpm("xx$$xxx$$xx")
                .build();
        ByteplusGeneral.PredictCandidateItem candidateItem = ByteplusGeneral.PredictCandidateItem.newBuilder()
                .setId("item_id")
                .build();
        ByteplusGeneral.PredictRelatedItem relatedItem = ByteplusGeneral.PredictRelatedItem.newBuilder()
                .setId("item_id")
                .build();

        ByteplusGeneral.PredictExtra extra = ByteplusGeneral.PredictExtra.newBuilder()
                .putExtra("extra_key", "value")
                .build();

        return ByteplusGeneral.PredictRequest.newBuilder()
                .setUser(user)
                .setContext(context)
                .addCandidateItems(candidateItem)
                .setRelatedItem(relatedItem)
                .setExtra(extra)
                .build();
    }

    // Report the recommendation request result (actual exposure data) through the callback interface
    public static void callbackExample(ByteplusGeneral.PredictRequest predictRequest,
                                       ByteplusGeneral.PredictResponse predictResponse) {
        // # 需要与predict的scene保持一致
        String scene = "home";
        List<ByteplusGeneral.CallbackItem> callbackItems = conv2CallbackItem(predictResponse.getValue().getItemsList());
        ByteplusGeneral.CallbackRequest callbackRequest = ByteplusGeneral.CallbackRequest.newBuilder()
                .setPredictRequestId(predictResponse.getRequestId())
                // required, should be consistent with the uid passed in the recommendation request
                .setUid(predictRequest.getUser().getUid())
                // required，should be consistent with `scene` used in the recommendation request
                .setScene(scene)
                .addAllItems(callbackItems)
                .build();

        Option[] opts = {
                Option.withRequestId(UUID.randomUUID().toString()),
                Option.withTimeout(Duration.ofMillis(800)),
        };
        ByteplusGeneral.CallbackResponse callbackResponse = null;
        try {
            callbackResponse = client.callback(callbackRequest, opts);
        } catch (NetException | BizException e) {
            e.printStackTrace();
        } finally {
            System.out.printf("callback rsp info: %s \n", callbackResponse);
        }
    }

    private static List<ByteplusGeneral.CallbackItem> conv2CallbackItem(List<ByteplusGeneral.PredictResultItem> resultItems) {
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

}
