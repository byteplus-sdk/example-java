package byteplus.example.retailv2;


import byteplus.sdk.retailv2.protocol.ByteplusRetailv2.Product;
import byteplus.sdk.retailv2.protocol.ByteplusRetailv2.User;
import byteplus.sdk.retailv2.protocol.ByteplusRetailv2.UserEvent;

import java.util.*;

public class MockHelper {

    public static List<User> mockUsers(int count) {
        List<User> users = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            User user = mockUser();
            user = user.toBuilder().setUserId(user.getUserId() + i).build();
            users.add(user);
        }
        return users;
    }

    public static User mockUser() {
        User.Location location = User.Location.newBuilder()
                .setCountry("china")
                .setCity("beijing")
                .setDistrictOrArea("haidian")
                .setPostcode("123456")
                .build();

        Map<String, String> extra = new HashMap<>();
        extra.put("first_name", "first");

        return User.newBuilder()
                .setUserId("user_id")
                .setGender("male")
                .setAge("23")
                .addAllTags(Arrays.asList("tag1", "tag2", "tag3"))
                .setActivationChannel("AppStore")
                .setMembershipLevel("silver")
                .setRegistrationTimestamp(System.currentTimeMillis() / 1000)
                .setLocation(location)
                .putAllExtra(extra)
                .build();
    }

    public static List<Product> mockProducts(int count) {
        List<Product> products = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Product product = mockProduct();
            product = product.toBuilder().setProductId(product.getProductId() + i).build();
            products.add(product);
        }
        return products;
    }

    public static Product mockProduct() {
        Product.Category.CategoryNode category1Node1 = Product.Category.CategoryNode.newBuilder()
                .setIdOrName("cate_1_1")
                .build();
        Product.Category category1 = Product.Category.newBuilder()
                .setCategoryDepth(1)
                .addCategoryNodes(category1Node1)
                .build();
        Product.Category.CategoryNode category2Node1 = Product.Category.CategoryNode.newBuilder()
                .setIdOrName("cate_2_1")
                .build();
        Product.Category.CategoryNode category2Node2 = Product.Category.CategoryNode.newBuilder()
                .setIdOrName("cate_2_2")
                .build();
        Product.Category category2 = Product.Category.newBuilder()
                .setCategoryDepth(2)
                .addCategoryNodes(category2Node1)
                .addCategoryNodes(category2Node2)
                .build();

        Product.Brand brand1 = Product.Brand.newBuilder()
                .setBrandDepth(1)
                .setIdOrName("brand_1")
                .build();
        Product.Brand brand2 = Product.Brand.newBuilder()
                .setBrandDepth(2)
                .setIdOrName("brand_2")
                .build();

        Product.Price price = Product.Price.newBuilder()
                .setCurrentPrice(10)
                .setOriginPrice(10)
                .build();

        Product.Display display = Product.Display.newBuilder()
                .addAllDetailPageDisplayTags(Arrays.asList("tag1", "tag2"))
                .addAllListingPageDisplayTags(Arrays.asList("taga", "tagb"))
                .setListingPageDisplayType("image")
                .setCoverMultimediaUrl("https://www.google.com")
                .build();

        Product.ProductSpec spec = Product.ProductSpec.newBuilder()
                .setProductGroupId("group_id")
                .setUserRating(0.23)
                .setCommentCount(100)
                .setSource("self")
                .setPublishTimestamp(System.currentTimeMillis() / 1000)
                .build();

        Product.Seller seller = Product.Seller.newBuilder()
                .setId("seller_id")
                .setSellerLevel("level1")
                .setSellerRating(3.5)
                .build();

        return Product.newBuilder()
                .setProductId("product_id")
                .addCategories(category1)
                .addCategories(category2)
                .addBrands(brand1)
                .addBrands(brand2)
                .setPrice(price)
                .setIsRecommendable(true)
                .setTitle("title")
                .setQualityScore(3.4)
                .addAllTags(Arrays.asList("tag1", "tag2", "tag3"))
                .setDisplay(display)
                .setProductSpec(spec)
                .setSeller(seller)
                .putExtra("count", "20")
                .build();
    }

    public static List<UserEvent> mockUserEvents(int count) {
        List<UserEvent> userEvents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UserEvent userEvent = mockUserEvent();
            userEvents.add(userEvent);
        }
        return userEvents;
    }

    public static UserEvent mockUserEvent() {
        UserEvent.Scene scene = UserEvent.Scene.newBuilder()
                .setSceneName("scene_name")
                .setPageNumber(2)
                .setOffset(10)
                .build();

        UserEvent.Device device = mockDevice();

        UserEvent.Context context = UserEvent.Context.newBuilder()
                .setQuery("query")
                .setRootProductId("root_product_id")
                .build();

        return UserEvent.newBuilder()
                .setUserId("user_id")
                .setEventType("purchase")
                .setEventTimestamp(System.currentTimeMillis() / 1000)
                .setScene(scene)
                .setProductId("product_id")
                .setDevice(device)
                .setContext(context)
                .setAttributionToken("attribution_token")
                .setRecInfo("trans_data")
                .setTrafficSource("self")
                .setPurchaseCount(20)
                .putExtra("children", "true")
                .build();
    }

    public static UserEvent.Device mockDevice() {
        return UserEvent.Device.newBuilder()
                .setPlatform("android")
                .setOsType("phone")
                .setAppVersion("app_version")
                .setDeviceModel("device_model")
                .setDeviceBrand("device_brand")
                .setOsVersion("os_version")
                .setBrowserType("firefox")
                .setUserAgent("user_agent")
                .setNetwork("3g")
                .build();
    }
}
