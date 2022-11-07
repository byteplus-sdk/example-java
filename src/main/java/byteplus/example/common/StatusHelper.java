package byteplus.example.common;


import byteplus.sdk.common.protocol.ByteplusCommon.Status;

import static byteplus.sdk.core.Constant.STATUS_CODE_IDEMPOTENT;
import static byteplus.sdk.core.Constant.STATUS_CODE_OPERATION_LOSS;
import static byteplus.sdk.core.Constant.STATUS_CODE_SUCCESS;
import static byteplus.sdk.core.Constant.STATUS_CODE_TOO_MANY_REQUEST;

public class StatusHelper {
    public static final Integer HTTP_STATUS_OK = 200;

    public static boolean isUploadSuccess(Status status) {
        int code = status.getCode();
        // It is still considered as success, which is rejected for idempotent
        return code == STATUS_CODE_SUCCESS || code == STATUS_CODE_IDEMPOTENT;
    }

    public static boolean isSuccess(Status status) {
        int code = status.getCode();
        return code == STATUS_CODE_SUCCESS || code == HTTP_STATUS_OK;
    }

    public static boolean isSuccess(int code) {
        return code == STATUS_CODE_SUCCESS|| code == HTTP_STATUS_OK;
    }

    public static boolean isServerOverload(Status status) {
        return status.getCode() == STATUS_CODE_TOO_MANY_REQUEST;
    }

    public static boolean isLossOperation(Status status) {
        return status.getCode() == STATUS_CODE_OPERATION_LOSS;
    }
}
