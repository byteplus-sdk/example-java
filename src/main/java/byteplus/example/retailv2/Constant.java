package byteplus.example.retailv2;

public final class Constant {
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
    public final static String TENANT = "retail_demo";
}
