package byteplus.example.byteair;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockHelper {

    public static List<Map<String, Object>> mockDataList(int count) {
        List<Map<String, Object>> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, Object> data = mockData();
            dataList.add(data);
        }
        return dataList;
    }

    public static Map<String, Object> mockData() {
        Map<String, Object> result = new HashMap<>();
        result.put("user_id", "1457789");
        result.put("event_type", "purchase");
        result.put("event_timestamp", 1623681767);
        result.put("scene_scene_name", "product detail page");
        result.put("scene_page_number", 2);
        result.put("scene_offset", 10);
        result.put("product_id", "632461");
        result.put("device_platform", "android");
        result.put("device_os_type", "phone");
        result.put("device_app_version", "9.2.0");
        result.put("device_device_model", "huawei-mate30");
        result.put("device_device_brand", "huawei");
        result.put("device_os_version", "10");
        result.put("device_browser_type", "chrome");
        result.put("device_user_agent", "Mozilla/5.0 (Linux; Android 10; TAS-AN00; HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36");
        result.put("device_network", "3g");
        result.put("context_query", "");
        result.put("context_root_product_id", "441356");
        result.put("attribution_token", "eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ");
        result.put("rec_info", "CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg==");
        result.put("traffic_source", "self");
        result.put("purchase_count", 20);
        result.put("extra", "{\"session_id\":\"sess_89j9ifuqrbplk0rti2va2k1ha0\",\"request_id\":\"860ae3f6-7e4d-43a9-8699-114cbd72c287\"}");
        return result;
    }
}
