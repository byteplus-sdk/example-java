package byteplus.example.retail;

import byteplus.retail.sdk.protocol.ByteplusRetail.GetOperationRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.Operation;
import byteplus.retail.sdk.protocol.ByteplusRetail.OperationResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.Status;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
import byteplus.sdk.core.Options;
import byteplus.sdk.retail.RetailClient;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;

import static byteplus.sdk.core.Constant.IDEMPOTENT_STATUS_CODE;
import static byteplus.sdk.core.Constant.OPERATION_LOSS_STATUS_CODE;
import static byteplus.sdk.core.Constant.SUCCESS_STATUS_CODE;
import static byteplus.sdk.core.Constant.TOO_MANY_REQUEST_STATUS_CODE;

@Slf4j
public class RequestHelper {
    // The maximum time for polling the execution results of the import task
    private final static Duration POLLING_TIMEOUT = Duration.ofSeconds(10);

    // The time interval between requests during polling
    private final static int POLLING_INTERVAL_MILLIS = 100;

    // The interval base of retry for server overload
    private final static int OVERLOAD_RETRY_INTERVAL_MILLIS = 100;

    private static final Duration GET_OPERATION_TIMEOUT = Duration.ofMillis(600);

    private final RetailClient client;

    public RequestHelper(RetailClient client) {
        this.client = client;
    }

    interface Callable<Rsp extends Message, Req extends Message> {
        Rsp call(Req req, Options.Filler... opts) throws BizException, NetException;
    }

    /**
     * If the task is submitted too fast or the server is overloaded,
     * the server may refuse the request. In order to ensure the accuracy
     * of data transmission, you should wait some time and request again,
     * but it cannot retry endlessly. The maximum count of retries should be set.
     *
     * @param callable the task need to execute
     * @param <Rsp>    the response type of task
     * @return the response of task
     * @throws BizException throw by task
     */
    public <Rsp extends Message, Req extends Message> Rsp doWithRetryAlthoughOverload(
            Callable<Rsp, Req> callable,
            Req req,
            Options.Filler[] opts,
            int retryTimes) throws BizException {

        int tryTimes = retryTimes + 1;
        for (int i = 0; i < tryTimes; i++) {
            Rsp response = doWithRetry(callable, req, opts, retryTimes - i);
            if (isRejectForOverload(getStatus(response))) {
                try {
                    // Wait some time before making a request,
                    // and the wait time increases with the number of retries
                    Thread.sleep(randomWaitTime(i));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return response;
                }
                continue;
            }
            return response;
        }
        throw new BizException("Invalid retry times");
    }

    private Status getStatus(Object response) {
        Class<?> clz = response.getClass();
        try {
            Method getStatusMethod = clz.getMethod("getStatus");
            return (Status) getStatusMethod.invoke(response);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.error("Response not contain status field, msg:{}", e.getMessage());
        }
        return Status.newBuilder()
                .setCode(SUCCESS_STATUS_CODE)
                .build();
    }

    private boolean isRejectForOverload(Status status) {
        return status.getCode() == TOO_MANY_REQUEST_STATUS_CODE;
    }

    private int randomWaitTime(int retriedTimes) {
        final int INCR_SPEED = 3;
        if (retriedTimes < 0) {
            return OVERLOAD_RETRY_INTERVAL_MILLIS;
        }
        double rate = 1 + Math.random() * Math.pow(INCR_SPEED, retriedTimes);
        return (int) (OVERLOAD_RETRY_INTERVAL_MILLIS * rate);
    }

    public <Rsp extends Message, Req extends Message> Rsp doWithRetry(
            Callable<Rsp, Req> callable,
            Req req,
            Options.Filler[] opts,
            int retryTimes) throws BizException {

        Rsp rsp = null;
        // To ensure the request is successfully received by the server,
        // it should be retried after a network exception occurs.
        // To prevent the retry from causing duplicate uploading same data,
        // the request should be retried by using the same requestId.
        // If a new requestId is used, it will be treated as a new request
        // by the server, which may save duplicate data
        opts = withRequestId(opts);
        int tryTimes = retryTimes + 1;
        for (int i = 0; i < tryTimes; i++) {
            try {
                rsp = callable.call(req, opts);
            } catch (NetException e) {
                if (i == tryTimes - 1) {
                    log.error("[DoRetryRequest] fail finally after retried {} times", tryTimes);
                    throw new BizException(e.getMessage());
                }
                continue;
            }
            break;
        }
        return rsp;
    }

    private Options.Filler[] withRequestId(Options.Filler[] opts) {
        Options.Filler[] optsWithRequestId;
        if (Objects.isNull(opts)) {
            optsWithRequestId = new Options.Filler[1];
        } else {
            optsWithRequestId = new Options.Filler[opts.length + 1];
        }
        // This will not override the RequestId set by the user
        optsWithRequestId[0] = Option.withRequestId(UUID.randomUUID().toString());
        System.arraycopy(opts, 0, optsWithRequestId, 1, opts.length);
        return optsWithRequestId;
    }

    public <Rsp extends Message, Req extends Message> Rsp doImport(
            Callable<OperationResponse, Req> callable,
            Req req,
            Options.Filler[] opts,
            Parser<Rsp> parser,
            int retryTimes) throws BizException {

        // To ensure that the request is successfully received by the server,
        // it should be retried after a network exception occurs.
        OperationResponse opRsp = doWithRetryAlthoughOverload(callable, req, opts, retryTimes);

        return pollingResponse(parser, opRsp);
    }

    private <Rsp extends Message> Rsp pollingResponse(
            Parser<Rsp> rspParser, OperationResponse opRsp) throws BizException {
        if (!isSuccess(opRsp)) {
            log.error("[PollingImportResponse] server return error info: \n{}", opRsp.getStatus());
            throw new BizException(opRsp.getStatus().getMessage());
        }
        Any responseAny = doPollingResponse(opRsp.getOperation().getName());
        try {
            return rspParser.parseFrom(responseAny.getValue().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            log.error("[PollingImportResponse] parse response by protobuf fail, {}", e.getMessage());
            throw new BizException("parse import response fail");
        }
    }

    private boolean isSuccess(OperationResponse opRsp) {
        int code = opRsp.getStatus().getCode();
        return code == SUCCESS_STATUS_CODE || code == IDEMPOTENT_STATUS_CODE;
    }

    private Any doPollingResponse(String name) throws BizException {
        OperationResponse opRsp;
        Operation operation;
        // Set the polling expiration time to prevent endless polling
        LocalTime pollingExpireTime = LocalTime.now().plus(POLLING_TIMEOUT);
        while (LocalTime.now().isBefore(pollingExpireTime)) {
            // Request the Get Operation interface to Get the latest Operation
            opRsp = getPollingOperation(name);
            if (Objects.isNull(opRsp)) {
                // When polling for import results, you should continue polling
                // until the maximum polling time is exceeded, as long as there is
                // no obvious error that should not continue, such as server telling
                // operation lost, parse response body fail, etc
                continue;
            }
            // The server may lose operation information due to unexpected failure.
            // At this time, should interrupt the request and send feedback to bytedance
            // to confirm whether the data in this request has been successfully imported
            if (isLossOperation(opRsp)) {
                log.error("[PollingResponse] operation loss, msg:{}", opRsp.getStatus().getMessage());
                throw new BizException("operation loss, please feedback to bytedance");
            }
            operation = opRsp.getOperation();
            // The task corresponding to this operation has been completed,
            // and the execution result  can be obtained through "operation.response"
            if (operation.getDone()) {
                return operation.getResponse();
            }
            try {
                // Pause some time to prevent server overload
                Thread.sleep(POLLING_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                throw new BizException(e.getMessage());
            }
        }
        log.error("[PollingResponse] timeout after {}", POLLING_TIMEOUT);
        throw new BizException("polling operation result timeout");
    }

    private OperationResponse getPollingOperation(String name) throws BizException {
        GetOperationRequest request = GetOperationRequest
                .newBuilder()
                .setName(name)
                .build();
        try {
            return client.getOperation(request, Option.withTimeout(GET_OPERATION_TIMEOUT));
        } catch (NetException e) {
            // An exception should not be thrown.
            // Throwing an exception means the request could not continue
            // When polling for import results, you should continue polling
            // until the maximum polling time is exceeded, as long as there is
            // no obvious error that should not continue, such as server telling
            // operation lost, parse response body fail, etc
            return null;
        }
    }

    private static boolean isLossOperation(OperationResponse opRsp) {
        return opRsp.getStatus().getCode() == OPERATION_LOSS_STATUS_CODE;
    }
}
