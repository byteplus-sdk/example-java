package byteplus.example.byteair;

public final class Constant {
    /**
     * 租户相关信息
     */
    //字节侧提供，用于签名
    public final static String TOKEN = "xxxxxxxxx";

    //火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
    public final static String TENANT_ID = "xxxxxx";

    //个性化推荐服务新建的项目id(project_id)，如"1231314"
    public final static String TENANT = "xxxxxx";

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
}
