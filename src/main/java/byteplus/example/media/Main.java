package byteplus.example.media;

import byteplus.example.common.RequestHelper;
import byteplus.example.common.StatusHelper;
import byteplus.sdk.common.protocol.ByteplusCommon;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Region;
import byteplus.sdk.media.MediaClient;
import byteplus.sdk.media.MediaClientBuilder;
import byteplus.sdk.media.protocol.ByteplusMedia.User;
import byteplus.sdk.media.protocol.ByteplusMedia.Content;
import byteplus.sdk.media.protocol.ByteplusMedia.UserEvent;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUsersRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUsersResponse;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteContentsRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteContentsResponse;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUserEventsRequest;
import byteplus.sdk.media.protocol.ByteplusMedia.WriteUserEventsResponse;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

@Slf4j
public class Main {
    private final static MediaClient client;

    private final static ConcurrentHelper concurrentHelper;

    private final static String DEFAULT_DONE_TOPIC = "user";

    private final static Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_DONE_TIMEOUT = Duration.ofMillis(1000);

    // A unique token assigned by bytedance, which is used to
    // generate an authenticated signature when building a request.
    // It is sometimes called "secret".
    public final static String TOKEN = "xxxxxxxxxxxxxxxx";

    // A unique ID assigned by Bytedance, which is used to
    // generate an authenticated signature when building a request
    // It is sometimes called "appkey".
    public final static String TENANT_ID = "xxxxxxxxxxxx";

    // A unique identity assigned by Bytedance, which is need to fill in URL.
    // It is sometimes called "company".
    public final static String TENANT = "media_demo";


    static {
        client = new MediaClientBuilder()
                .tenant(TENANT) // Required
                .tenantId(TENANT_ID) // Required
                .token(TOKEN) // Required
                .region(Region.SG) //Required, select enum value according to your region
//                .schema("https") //Optional
//                .headers(Collections.singletonMap("Customer-Header", "value")) // Optional
                .build();
        concurrentHelper = new ConcurrentHelper(client);
    }

    /**
     * Those examples request server with account named 'media_demo',
     * The data in the "demo" account is only used for testing
     * and communication between customers.
     * Please don't send your private data by "demo" account.
     */
    public static void main(String[] args) {
        // Write real-time user data
        writeUsersExample();
        // Write real-time user data concurrently
        concurrentWriteUsersExample();

        // Write real-time content data
        writeContentsExample();
        // Write real-time content data concurrently
        concurrentWriteContentsExample();

        // Write real-time user event data
        writeUserEventsExample();
        // Write real-time user event data concurrently
        concurrentWriteUserEventsExample();

        // done
        doneExample();

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
            response = client.writeUsers(request, opts);
        } catch (BizException | NetException e) {
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

    public static void writeContentsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteContentsRequest request = buildWriteContentsRequest(1);
        Option[] options = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        WriteContentsResponse response;
        try {
            response = client.writeContents(request, options);
        } catch (BizException | NetException e) {
            log.error("write content occur err, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isUploadSuccess(response.getStatus())) {
            log.info("write content success");
            return;
        }
        log.error("write content find failure info, msg:{} errItems:{}",
                response.getStatus(), response.getErrorsList());
    }

    public static void concurrentWriteContentsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteContentsRequest request = buildWriteContentsRequest(1);
        Option[] opts = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        concurrentHelper.submitRequest(request, opts);
    }

    private static WriteContentsRequest buildWriteContentsRequest(int count) {
        List<Content> contents = MockHelper.mockContents(count);
        return WriteContentsRequest.newBuilder()
                .addAllContents(contents)
                //.putExtra("extra_info", "info")
                .build();
    }

    public static void writeUserEventsExample() {
        // The "WriteXXX" api can transfer max to 2000 items at one request
        WriteUserEventsRequest request = buildWriteUserEventsRequest(1);
        Option[] options = defaultOptions(DEFAULT_WRITE_TIMEOUT);
        WriteUserEventsResponse response;
        try {
            response = client.writeUserEvents(request, options);
        } catch (BizException | NetException e) {
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
                .putExtra("extra_info", "info")
                .build();
    }

    // 离线天级数据上传完成后Done接口example
    private static void doneExample() {
        LocalDate date = LocalDate.of(2021, 12, 10);
        List<LocalDate> dateList = Collections.singletonList(date);
        Option[] opts = defaultOptions(DEFAULT_DONE_TIMEOUT);
        RequestHelper.Callable<ByteplusCommon.DoneResponse, List<LocalDate>> call
                = (req, optList) -> client.done(req, DEFAULT_DONE_TOPIC, optList);
        ByteplusCommon.DoneResponse response;
        try {
            response = client.done(dateList, DEFAULT_DONE_TOPIC, opts);
        } catch (BizException | NetException e) {
            log.error("[Done] occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("[Done] success");
            return;
        }
        log.error("[Done] find failure info, rsp:{}", response);
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
