package byteplus.example.retail;

import byteplus.example.common.Example;
import byteplus.example.common.RequestHelper;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.common.protocol.ByteplusCommon.Operation;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.retail.RetailClient;
import byteplus.sdk.retail.RetailClientBuilder;
import byteplus.sdk.retail.protocol.ByteplusRetail.AckServerImpressionsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.AckServerImpressionsRequest.AlteredProduct;
import byteplus.sdk.retail.protocol.ByteplusRetail.DateConfig;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportProductsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportProductsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUserEventsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUserEventsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUsersRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.ImportUsersResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.PredictRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.PredictResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.PredictResult;
import byteplus.sdk.retail.protocol.ByteplusRetail.PredictResult.ResponseProduct;
import byteplus.sdk.retail.protocol.ByteplusRetail.Product;
import byteplus.sdk.retail.protocol.ByteplusRetail.ProductsInlineSource;
import byteplus.sdk.retail.protocol.ByteplusRetail.ProductsInputConfig;
import byteplus.sdk.retail.protocol.ByteplusRetail.User;
import byteplus.sdk.retail.protocol.ByteplusRetail.UserEvent;
import byteplus.sdk.retail.protocol.ByteplusRetail.UserEventsInlineSource;
import byteplus.sdk.retail.protocol.ByteplusRetail.UserEventsInputConfig;
import byteplus.sdk.retail.protocol.ByteplusRetail.UsersInlineSource;
import byteplus.sdk.retail.protocol.ByteplusRetail.UsersInputConfig;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteProductsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteProductsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUserEventsRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUserEventsResponse;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUsersRequest;
import byteplus.sdk.retail.protocol.ByteplusRetail.WriteUsersResponse;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class Main {
    private final static RetailClient client;

    private final static RequestHelper requestHelper;

    private final static ConcurrentHelper concurrentHelper;

    private final static int DEFAULT_RETRY_TIMES = 2;

    private final static Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_IMPORT_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_PREDICT_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_ACK_IMPRESSIONS_TIMEOUT = Duration.ofMillis(800);

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
    public final static String TENANT = "retail_demo";


    static {
//        // Metrics configuration, when Metrics and Metrics Log are turned on,
//        // the metrics and logs at runtime will be collected and sent to the byteplus server.
//        // During debugging, byteplus can help customers troubleshoot problems.
//        MetricsCfg metricsCfg = new MetricsCfg().toBuilder()
//                .enableMetrics(true) // enable metrics, default is false.
//                .enableMetricsLog(true) // enable metrics log, default is false.
//                // The time interval for reporting metrics to the byteplus server, the default is 15s.
//                // When the QPS is high, the value of the reporting interval can be reduced to prevent
//                // loss of metrics.
//                // The longest should not exceed 30s, otherwise it will cause the loss of metrics accuracy.
//                .reportInterval(Duration.ofSeconds(15))
//                .build();


//        HostAvailabler.Config config = new HostAvailabler.Config().toBuilder()
//                // The timeout for sending ping requests when hostAvailabler sorts the host, default is 300ms.
//                .pingTimeout(Duration.ofMillis(300))
//                // The interval for sending ping requests when hostAvailabler sorts the host, default is 1s.
//                .pingInterval(Duration.ofSeconds(1))
//                .build();

        client = new RetailClientBuilder()
                .tenant(TENANT) // Required
                .tenantId(TENANT_ID) // Required
                .token(TOKEN) // Required
                .region(Region.SG) //Required, select enum value according to your region
//                .schema("https") //Optional
//                .headers(Collections.singletonMap("Customer-Header", "value")) // Optional
//                .metricsConfig(metricsCfg) // Optional
//                .hostAvailablerConfig(config) // Optional
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
        writeUsersExample();
        // Write real-time user data concurrently
        // concurrentWriteUsersExample();
        // Import daily offline user data
        importUsersExample();
        // Import daily offline user data concurrently
        // concurrentImportUsersExample();

        // Write real-time product data
        writeProductsExample();
        // Write real-time product data concurrently
        // concurrentWriteProductsExample();
        // Import daily offline product data
        importProductsExample();
        // Concurrent import daily offline product data
        // concurrentImportProductsExample();

        // Write real-time user event data
        writeUserEventsExample();
        // Write real-time user event data concurrently
        // concurrentWriteUserEventsExample();
        // Import daily offline user event data
        importUserEventsExample();
        // Concurrent import daily offline user event data
        // concurrentImportUserEventsExample();

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

        try {
            // Pause for 5 seconds until the asynchronous import task completes
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {

        }
        client.release();
        System.exit(0);
    }

    public static void writeUsersExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteUsersRequest request = buildWriteUsersRequest(1);
        Option[] opts = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        WriteUsersResponse response;
        try {
            response = requestHelper.doWithRetry(client::writeUsers, request, opts, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("write user occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write user success");
            return;
        }
        log.error("write user find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    public static void concurrentWriteUsersExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteUsersRequest request = buildWriteUsersRequest(1);
        Option[] opts = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static WriteUsersRequest buildWriteUsersRequest(int count) {
        List<User> users = MockHelper.mockUsers(count);
        return WriteUsersRequest.newBuilder()
                .addAllUsers(users)
                //.putExtra("extra_info", "info")
                .build();
    }

    public static void importUsersExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportUsersRequest request = buildImportUsersRequest(10);
        Parser<ImportUsersResponse> rspParser = ImportUsersResponse.parser();
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        ImportUsersResponse response;
        try {
            response = requestHelper.doImport(client::importUsers, request, opts, rspParser, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("import user occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("import user success");
            return;
        }
        log.error("import user find failure info, msg:{} errSamples:{}",
                response.getStatus(), response.getErrorSamplesList());
    }

    public static void concurrentImportUsersExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportUsersRequest request = buildImportUsersRequest(10);
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static ImportUsersRequest buildImportUsersRequest(int count) {
        UsersInlineSource inlineSource = UsersInlineSource.newBuilder()
                .addAllUsers(MockHelper.mockUsers(count))
                .build();
        UsersInputConfig inputConfig = UsersInputConfig.newBuilder()
                .setUsersInlineSource(inlineSource)
                .build();

        DateConfig dateConfig = DateConfig.newBuilder()
                .setDate(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .setIsEnd(false)
                .build();

        return ImportUsersRequest.newBuilder()
                .setInputConfig(inputConfig)
                .setDateConfig(dateConfig)
                .build();
    }

    public static void writeProductsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteProductsRequest request = buildWriteProductsRequest(1);
        Option[] options = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        WriteProductsResponse response;
        try {
            response = requestHelper.doWithRetry(client::writeProducts, request, options, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("write product occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write product success");
            return;
        }
        log.error("write product find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    public static void concurrentWriteProductsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteProductsRequest request = buildWriteProductsRequest(1);
        Option[] opts = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static WriteProductsRequest buildWriteProductsRequest(int count) {
        List<Product> products = MockHelper.mockProducts(count);
        return WriteProductsRequest.newBuilder()
                .addAllProducts(products)
                //.putExtra("extra_info", "info")
                .build();
    }

    public static void importProductsExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportProductsRequest request = buildImportProductsRequest(10);
        Parser<ImportProductsResponse> rspParser = ImportProductsResponse.parser();
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        ImportProductsResponse response;
        try {
            response = requestHelper.doImport(client::importProducts, request, opts, rspParser, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("import products occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("import products success");
            return;
        }
        log.error("import products find failure info, msg:{} errSamples:{}",
                response.getStatus(), response.getErrorSamplesList());
    }

    public static void concurrentImportProductsExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportProductsRequest request = buildImportProductsRequest(10);
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static ImportProductsRequest buildImportProductsRequest(int count) {
        ProductsInlineSource inlineSource = ProductsInlineSource.newBuilder()
                .addAllProducts(MockHelper.mockProducts(count))
                .build();
        ProductsInputConfig inputConfig = ProductsInputConfig.newBuilder()
                .setProductsInlineSource(inlineSource)
                .build();

        DateConfig dateConfig = DateConfig.newBuilder()
                .setDate(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .setIsEnd(false)
                .build();

        return ImportProductsRequest.newBuilder()
                .setInputConfig(inputConfig)
                .setDateConfig(dateConfig)
                .build();
    }

    public static void writeUserEventsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteUserEventsRequest request = buildWriteUserEventsRequest(1);
        Option[] options = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        WriteUserEventsResponse response;
        try {
            response = requestHelper.doWithRetry(client::writeUserEvents, request, options, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("write user events occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write user events success");
            return;
        }
        log.error("write user events find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    public static void concurrentWriteUserEventsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteUserEventsRequest request = buildWriteUserEventsRequest(1);
        Option[] opts = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static WriteUserEventsRequest buildWriteUserEventsRequest(int count) {
        List<UserEvent> userEvents = MockHelper.mockUserEvents(count);
        return WriteUserEventsRequest.newBuilder()
                .addAllUserEvents(userEvents)
                //.putExtra("extra_info", "info")
                .build();
    }

    public static void importUserEventsExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportUserEventsRequest request = buildImportUserEventsRequest(10);
        Parser<ImportUserEventsResponse> rspParser = ImportUserEventsResponse.parser();
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        ImportUserEventsResponse response;
        try {
            response = requestHelper.doImport(client::importUserEvents, request, opts, rspParser, DEFAULT_RETRY_TIMES);
        } catch (BizException e) {
            log.error("import user events occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("import user events success");
            return;
        }
        log.error("import user events find failure info, msg:{} errSamples:{}",
                response.getStatus(), response.getErrorSamplesList());
    }

    public static void concurrentImportUserEventsExample() {
        // The "ImportXXX" api can transfer max to 10k items at one request
        ImportUserEventsRequest request = buildImportUserEventsRequest(10);
        Option[] opts = defaultOptions(DEFAULT_IMPORT_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static ImportUserEventsRequest buildImportUserEventsRequest(int count) {
        UserEventsInlineSource inlineSource = UserEventsInlineSource.newBuilder()
                .addAllUserEvents(MockHelper.mockUserEvents(count))
                .build();

        UserEventsInputConfig inputConfig = UserEventsInputConfig.newBuilder()
                .setUserEventsInlineSource(inlineSource)
                .build();

        DateConfig dateConfig = DateConfig.newBuilder()
                .setDate(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .setIsEnd(false)
                .build();

        return ImportUserEventsRequest.newBuilder()
                .setInputConfig(inputConfig)
                .setDateConfig(dateConfig)
                .build();
    }

    public static void getOperationExample() {
        String name = "750eca88-5165-4aae-851f-a93b75a27b03";
        Example.getOperationExample(client, name);
    }

    public static void listOperationsExample() {
        String filter = "date>=2021-06-15 and worksOn=ImportUsers and done=true";
        List<Operation> operations = Example.listOperationsExample(client, filter);
        parseTaskResponse(operations);
    }

    private static void parseTaskResponse(List<Operation> operations) {
        if (Objects.isNull(operations) || operations.isEmpty()) {
            return;
        }
        for (Operation operation : operations) {
            if (!operation.getDone()) {
                continue;
            }
            Any responseAny = operation.getResponse();
            String typeUrl = responseAny.getTypeUrl();
            // To ensure compatibility, do not parse response by 'Any.unpack()'
            try {
                if (typeUrl.contains("ImportUsers")) {
                    ImportUsersResponse importUsersRsp;
                    importUsersRsp = ImportUsersResponse.parseFrom(responseAny.getValue());
                    log.info("[ListOperations] ImportUsers rsp:\n{}", importUsersRsp);
                } else if (typeUrl.contains("ImportProducts")) {
                    ImportProductsResponse importProductsRsp;
                    importProductsRsp = ImportProductsResponse.parseFrom(responseAny.getValue());
                    log.info("[ListOperations] ImportProducts rsp:\n{}", importProductsRsp);
                } else if (typeUrl.contains("ImportUserEvents")) {
                    ImportUserEventsResponse importUserEventsRsp;
                    importUserEventsRsp = ImportUserEventsResponse.parseFrom(responseAny.getValue());
                    log.info("[ListOperations] ImportUserEvents rsp:\n{}", importUserEventsRsp);
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
        Option[] predict_opts = defaultOptions(DEFAULT_PREDICT_TIMEOUT);
        PredictResponse response;
        try {
            // The "home" is scene name, which provided by ByteDance, usually is "home"
            response = client.predict(predictRequest, "home", predict_opts);
        } catch (Exception e) {
            log.error("predict occur error, msg:{}", e.getMessage());
            return;
        }
        if (!StatusHelper.isSuccess(response.getStatus())) {
            log.error("predict find failure info, msg:{}", response.getStatus());
            return;
        }
        log.info("predict success");
        // The items, which is eventually shown to user,
        // should send back to Bytedance for deduplication
        List<AlteredProduct> alteredProducts = doSomethingWithPredictResult(response.getValue());
        AckServerImpressionsRequest ackRequest =
                buildAckRequest(response.getRequestId(), predictRequest, alteredProducts);
        Option[] ack_opts = defaultOptions(DEFAULT_ACK_IMPRESSIONS_TIMEOUT);
        concurrentHelper.submitRequest(ackRequest, ack_opts);
    }

    private static PredictRequest buildPredictRequest() {
        UserEvent.Scene scene = UserEvent.Scene.newBuilder()
                .setSceneName("home")
                .build();

        Product rootProduct = MockHelper.mockProduct();

        UserEvent.Device device = MockHelper.mockDevice();

        PredictRequest.Context context = PredictRequest.Context.newBuilder()
                .setRootProduct(rootProduct)
                .setDevice(device)
                .addAllCandidateProductIds(Arrays.asList("pid1", "pid2"))
                .build();

        return PredictRequest.newBuilder()
                .setUserId("user_id")
                .setSize(20)
                .setScene(scene)
                .setContext(context)
                .putExtra("clear_impression", "true")
                .build();
    }

    private static List<AlteredProduct> doSomethingWithPredictResult(PredictResult predictResult) {
        // You can handle recommend results here,
        // such as filter, insert other items, sort again, etc.
        // The list of goods finally displayed to user and the filtered goods
        // should be sent back to bytedance for deduplication
        return conv2AlteredProducts(predictResult.getResponseProductsList());
    }

    @NotNull
    private static List<AlteredProduct> conv2AlteredProducts(List<ResponseProduct> products) {
        if (Objects.isNull(products) || products.isEmpty()) {
            return Collections.emptyList();
        }
        List<AlteredProduct> alteredProducts = new ArrayList<>(products.size());
        for (int i = 0; i < products.size(); i++) {
            ResponseProduct responseProduct = products.get(i);
            AlteredProduct alteredProduct = AlteredProduct.newBuilder()
                    .setAlteredReason("kept")
                    .setProductId(responseProduct.getProductId())
                    .setRank(i + 1)
                    .build();

            alteredProducts.add(alteredProduct);
        }
        return alteredProducts;
    }

    private static AckServerImpressionsRequest buildAckRequest(
            String predictRequestId,
            PredictRequest predictRequest,
            List<AlteredProduct> alteredProducts) {

        return AckServerImpressionsRequest.newBuilder()
                .setPredictRequestId(predictRequestId)
                .setUserId(predictRequest.getUserId())
                .setScene(predictRequest.getScene())
                // If it is the recommendation result from byteplus, traffic_source is byteplus,
                // if it is the customer's own recommendation result, traffic_source is self.
                .setTrafficSource("byteplus")
                .addAllAlteredProducts(alteredProducts)
                //.putExtra("ip", "127.0.0.1")
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
