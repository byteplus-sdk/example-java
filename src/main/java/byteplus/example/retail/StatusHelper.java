package byteplus.example.retail;

import byteplus.retail.sdk.protocol.ByteplusRetail;

import static byteplus.sdk.core.Constant.IDEMPOTENT_STATUS_CODE;
import static byteplus.sdk.core.Constant.SUCCESS_STATUS_CODE;

public class StatusHelper {
    public static boolean isWriteSuccess(ByteplusRetail.Status status) {
        int code = status.getCode();
        // It is still considered as success, which is rejected for idempotent
        return code == SUCCESS_STATUS_CODE || code == IDEMPOTENT_STATUS_CODE;
    }

    public static boolean isSuccess(ByteplusRetail.Status status) {
        int code = status.getCode();
        return code == SUCCESS_STATUS_CODE;
    }
}
