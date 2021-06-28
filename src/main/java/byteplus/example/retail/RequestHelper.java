package byteplus.example.retail;

import byteplus.retail.sdk.protocol.ByteplusRetail.GetOperationRequest;
import byteplus.retail.sdk.protocol.ByteplusRetail.Operation;
import byteplus.retail.sdk.protocol.ByteplusRetail.OperationResponse;
import byteplus.retail.sdk.protocol.ByteplusRetail.Status;
import byteplus.sdk.core.BizException;
import byteplus.sdk.core.NetException;
import byteplus.sdk.core.Option;
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

import static byteplus.sdk.core.Constant.STATUS_CODE_SUCCESS;

@Slf4j
public class RequestHelper {
    // The maximum time for polling the execution results of the import task
    private final static Duration POLLING_TIMEOUT = Duration.ofSeconds(10);

    // The time interval between requests during polling
    private final static Duration POLLING_INTERVAL = Duration.ofMillis(100);

    // The interval base of retry for server overload
    private final static Duration OVERLOAD_RETRY_INTERVAL = Duration.ofMillis(200);

    private final static Duration GET_OPERATION_TIMEOUT = Duration.ofMillis(500);

    private final RetailClient client;

    interface Callable<Rsp extends Message, Req extends Message> {
        Rsp call(Req req, Option... opts) throws BizException, NetException;
    }

    public RequestHelper(RetailClient client) {
        this.client = client;
    }

    public <Rsp extends Message, Req extends Message> Rsp doImport(
            Callable<OperationResponse, Req> callable,
            Req req,
            Option[] opts,
            Parser<Rsp> parser,
            int retryTimes) throws BizException {

        // To ensure that the request is successfully received by the server,
        // it should be retried after network or overload exception occurs.
        OperationResponse opRsp
                = doWithRetryAlthoughOverload(callable, req, opts, retryTimes);
        if (!StatusHelper.isUploadSuccess(opRsp.getStatus())) {
            log.error("[PollingImportResponse] server return error info, rsp:\n{}", opRsp);
            throw new BizException(opRsp.getStatus().getMessage());
        }
        return pollingResponse(opRsp, parser);
    }

    /**
     * If the task is submitted too fast or the server is overloaded,
     * the server may refuse the request. In order to ensure the accuracy
     * of data transmission, you should wait some time and request again,
     * but it cannot retry endlessly. The maximum count of retries should be set.
     *
     * @param callable the task need to execute
     * @param <Req>>   the request type of task
     * @param opts     the options need by the task
     * @return the response of task
     * @throws BizException throw by task or server still overload after retry
     */
    public <Rsp extends Message, Req extends Message> Rsp doWithRetryAlthoughOverload(
            Callable<Rsp, Req> callable,
            Req req,
            Option[] opts,
            int retryTimes) throws BizException {

        if (retryTimes < 0) {
            retryTimes = 0;
        }
        int tryTimes = retryTimes + 1;
        for (int i = 0; i < tryTimes; i++) {
            Rsp response = doWithRetry(callable, req, opts, retryTimes - i);
            if (StatusHelper.isServerOverload(getStatus(response))) {
                try {
                    // Wait some time before request again,
                    // and the wait time will increase by the number of retried
                    Thread.sleep(randomOverloadWaitTime(i));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return response;
                }
                continue;
            }
            return response;
        }
        throw new BizException("Server overload");
    }

    public <Rsp extends Message, Req extends Message> Rsp doWithRetry(
            Callable<Rsp, Req> callable,
            Req req,
            Option[] opts,
            int retryTimes) throws BizException {

        Rsp rsp = null;
        // To ensure the request is successfully received by the server,
        // it should be retried after a network exception occurs.
        // To prevent the retry from causing duplicate uploading same data,
        // the request should be retried by using the same requestId.
        // If a new requestId is used, it will be treated as a new request
        // by the server, which may save duplicate data
        opts = withRequestId(opts);
        if (retryTimes < 0) {
            retryTimes = 0;
        }
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

    private Option[] withRequestId(Option[] opts) {
        Option[] optsWithRequestId;
        if (Objects.isNull(opts)) {
            optsWithRequestId = new Option[1];
        } else {
            optsWithRequestId = new Option[opts.length + 1];
        }
        // This will not override the RequestId set by the user
        optsWithRequestId[0] = Option.withRequestId(UUID.randomUUID().toString());
        if (Objects.nonNull(opts) && opts.length > 0) {
            System.arraycopy(opts, 0, optsWithRequestId, 1, opts.length);
        }
        return optsWithRequestId;
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
                .setCode(STATUS_CODE_SUCCESS)
                .build();
    }

    private long randomOverloadWaitTime(int retriedTimes) {
        final int INCREASE_SPEED = 3;
        if (retriedTimes < 0) {
            return RequestHelper.OVERLOAD_RETRY_INTERVAL.toMillis();
        }
        double rate = 1 + Math.random() * Math.pow(INCREASE_SPEED, retriedTimes);
        return (int) (RequestHelper.OVERLOAD_RETRY_INTERVAL.toMillis() * rate);
    }

    private <Rsp extends Message> Rsp pollingResponse(
            OperationResponse opRsp, Parser<Rsp> rspParser) throws BizException {
        Any responseAny = doPollingResponse(opRsp.getOperation().getName());
        try {
            return rspParser.parseFrom(responseAny.getValue().toByteArray());
        } catch (InvalidProtocolBufferException e) {
            log.error("[PollingResponse] parse response fail, {}", e.getMessage());
            throw new BizException("parse import response fail");
        }
    }

    private Any doPollingResponse(String name) throws BizException {
        // Set the polling expiration time to prevent endless polling
        LocalTime endTime = LocalTime.now().plus(POLLING_TIMEOUT);
        do {
            // Request the Get Operation interface to Get the latest Operation
            OperationResponse opRsp = getPollingOperation(name);
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
            if (StatusHelper.isLossOperation(opRsp.getStatus())) {
                log.error("[PollingResponse] operation loss, rsp:\n{}", opRsp);
                throw new BizException("operation loss, please feedback to bytedance");
            }
            Operation operation = opRsp.getOperation();
            // The task corresponding to this operation has been completed,
            // and the execution result  can be obtained through "operation.response"
            if (operation.getDone()) {
                return operation.getResponse();
            }
            try {
                // Pause some time to prevent server overload
                Thread.sleep(POLLING_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                throw new BizException(e.getMessage());
            }
        } while (LocalTime.now().isBefore(endTime));
        log.error("[PollingResponse] timeout after {}", POLLING_TIMEOUT);
        throw new BizException("polling import result timeout");
    }

    private OperationResponse getPollingOperation(String name) throws BizException {
        GetOperationRequest request = GetOperationRequest
                .newBuilder()
                .setName(name)
                .build();
        try {
            return client.getOperation(request, Option.withTimeout(GET_OPERATION_TIMEOUT));
        } catch (NetException e) {
            log.warn("[PollingResponse] get operation fail, name:{} msg:{}", name, e.getMessage());
            // The NetException should not be thrown.
            // Throwing an exception means the request could not continue,
            // while polling for import results should be continue until the
            // maximum polling time is exceeded, as long as there is no obvious
            // error that should not continue, such as server telling operation lost,
            // parse response body fail, etc.
            return null;
        }
    }
}
