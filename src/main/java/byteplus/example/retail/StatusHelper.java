package byteplus.example.retail;

import byteplus.retail.sdk.protocol.ByteplusRetail;

import static byteplus.sdk.core.Constant.STATUS_CODE_IDEMPOTENT;
import static byteplus.sdk.core.Constant.STATUS_CODE_OPERATION_LOSS;
import static byteplus.sdk.core.Constant.STATUS_CODE_SUCCESS;
import static byteplus.sdk.core.Constant.STATUS_CODE_TOO_MANY_REQUEST;

public class StatusHelper {
    public static boolean isUploadSuccess(ByteplusRetail.Status status) {
        int code = status.getCode();
        // It is still considered as success, which is rejected for idempotent
        return code == STATUS_CODE_SUCCESS || code == STATUS_CODE_IDEMPOTENT;
    }

    public static boolean isSuccess(ByteplusRetail.Status status) {
        int code = status.getCode();
        return code == STATUS_CODE_SUCCESS;
    }

    public static boolean isServerOverload(ByteplusRetail.Status status) {
        return status.getCode() == STATUS_CODE_TOO_MANY_REQUEST;
    }

    public static boolean isLossOperation(ByteplusRetail.Status status) {
        return status.getCode() == STATUS_CODE_OPERATION_LOSS;
    }
}
