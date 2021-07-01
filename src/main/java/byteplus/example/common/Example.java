package byteplus.example.common;

import byteplus.sdk.common.CommonClient;
import byteplus.sdk.common.protocol.ByteplusCommon.GetOperationRequest;
import byteplus.sdk.common.protocol.ByteplusCommon.ListOperationsRequest;
import byteplus.sdk.common.protocol.ByteplusCommon.ListOperationsResponse;
import byteplus.sdk.common.protocol.ByteplusCommon.Operation;
import byteplus.sdk.common.protocol.ByteplusCommon.OperationResponse;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

@Slf4j
public class Example {

    private final static Duration DEFAULT_GET_OPERATION_TIMEOUT = Duration.ofMillis(800);

    private final static Duration DEFAULT_LIST_OPERATIONS_TIMEOUT = Duration.ofMillis(800);

    public static void getOperationExample(CommonClient client, String name) {
        GetOperationRequest request = GetOperationRequest.newBuilder()
                .setName(name)
                .build();
        Option[] opts = new Option[]{
                Option.withTimeout(DEFAULT_GET_OPERATION_TIMEOUT),
        };
        OperationResponse response;
        try {
            response = client.getOperation(request, opts);
        } catch (NetException | BizException e) {
            log.error("get operation occur error, msg:{}", e.getMessage());
            return;
        }
        if (StatusHelper.isSuccess(response.getStatus())) {
            log.info("get operation success rsp:\n{}", response);
            return;
        }
        if (StatusHelper.isLossOperation(response.getStatus())) {
            log.error("operation loss, name:{}", request.getName());
            return;
        }
        log.error("get operation find failure info, rsp:\n{}", response);
    }

    public static List<Operation> listOperationsExample(CommonClient client) {
        // The "pageToken" is empty when getting the first page
        ListOperationsRequest request = buildListOperationsRequest("");
        Option[] opts = new Option[]{
                Option.withTimeout(DEFAULT_LIST_OPERATIONS_TIMEOUT),
        };
        ListOperationsResponse response;
        try {
            response = client.listOperations(request, opts);
        } catch (Exception e) {
            log.error("list operations occur err, msg:{}", e.getMessage());
            return null;
        }
        if (!StatusHelper.isSuccess(response.getStatus())) {
            log.error("list operations find failure info, msg:\n{}", response.getStatus());
            return null;
        }
        log.info("list operations success");
        return response.getOperationsList();
        // When continue getting next Page, the "pageToken" need be set,
        // whose value equals to previous request's `nextPageToken`.
        // ListOperationsRequest nextPageRequest = buildListOperationsRequest(response.getNextPageToken());
        // request next page
    }

    private static ListOperationsRequest buildListOperationsRequest(String pageToken) {
        String filter = "date>=2021-06-15 and worksOn=ImportUsers and done=true";
        return ListOperationsRequest.newBuilder()
                .setFilter(filter)
                .setPageSize(3)
                .setPageToken(pageToken)
                .build();
    }
}
