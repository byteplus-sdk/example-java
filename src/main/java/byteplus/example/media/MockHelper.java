package byteplus.example.media;

import byteplus.sdk.media.protocol.ByteplusMedia.User;
import byteplus.sdk.media.protocol.ByteplusMedia.Content;
import byteplus.sdk.media.protocol.ByteplusMedia.UserEvent;

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
        Map<String, String> extra = new HashMap<>();
        extra.put("additionalProp1", "additionalVal1");

        return User.newBuilder()
                .setUserId("1457789")
                .setGender("female")
                .setAge("18-25")
                .addAllTags(Arrays.asList("new user", "low purchasing power", "bargain seeker"))
                .setDeviceId("abc123")
                .setDeviceType("app")
                .setSubscriberType("free")
                .setLanguage("English")
                .addAllHistory(Arrays.asList("632461", "632462"))
                .setActivationChannel("AppStore")
                .setMembershipLevel("silver")
                .setRegistrationTimestamp(1623593487)
                .setCountry("USA")
                .setCity("Kirkland")
                .setDistrictOrArea("King County")
                .setPostcode("98033")
                //.putAllExtra(extra)
                .build();
    }

    public static List<Content> mockContents(int count) {
        List<Content> contents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Content content = mockContent();
            content = content.toBuilder().setContentId(content.getContentId() + i).build();
            contents.add(content);
        }
        return contents;
    }

    public static Content mockContent() {
        return Content.newBuilder()
                .setContentId("632461")
                .setIsRecommendable(1)
                .setCategories("[{\"category_depth\":1,\"category_nodes\":[{\"id_or_name\":\"Movie\"}]},{\"category_depth\":2,\"category_nodes\":[{\"id_or_name\":\" Comedy\"}]}]")
                .setContentTitle("Video #1")
                .setDescription("This is a test video")
                .setContentType("video")
                .setContentOwner("testuser#1")
                .setLanguage("English")
                .addAllTags(Arrays.asList("New", "Trending"))
                .addAllListingPageDisplayTags(Arrays.asList("popular", "recommend"))
                .addAllDetailPageDisplayTags(Arrays.asList("popular", "recommend"))
                .setListingPageDisplayType("image")
                .setCoverMultimediaUrl("https://images-na.ssl-images-amazon.com/images/I/81WmojBxvbL._AC_UL1500_.jpg")
                .setUserRating(3.0)
                .setViewsCount(10000)
                .setCommentsCount(100)
                .setLikesCount(1000)
                .setSharesCount(50)
                .setIsPaidContent(1)
                .setOriginPrice(12300)
                .setCurrentPrice(12100)
                .setPublishRegion("US")
                .addAllAvailableRegion(Arrays.asList("Singapore", "India", "US"))
                .setEntityId("1")
                .setEntityName("Friends")
                .setSeriesId("11")
                .setSeriesIndex(1)
                .setSeriesName("Friends Season 1")
                .setSeriesCount(10)
                .setVideoId("111")
                .setVideoIndex(6)
                .setVideoName("The One With Ross' New Girlfriend")
                .setVideoCount(10)
                .setVideoType("series")
                .setVideoDuration(2400000)
                .setPublishTimestamp(1623193487)
                .setCopyrightStartTimestamp(1623193487)
                .setCopyrightEndTimestamp(1623493487)
                .addAllActors(Arrays.asList("Rachel Green", "Ross Geller"))
                .setSource("self")
                //.putExtra("additionalProp1", "additionalVal1")
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
        return UserEvent.newBuilder()
                .setUserId("1457789")
                .setEventType("impression")
                .setEventTimestamp(1623681888)
                .setContentId("632461")
                .setTrafficSource("self")
                .setRequestId("67a9fcf74a82fdc55a26ab4ee12a7b96890407fc0042f8cc014e07a4a560a9ac")
                .setRecInfo("CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg==")
                .setAttributionToken("eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ")
                .setSceneName("Home Page")
                .setPageNumber(2)
                .setOffset(10)
                .setPlayType("0")
                .setPlayDuration(6000)
                .setStartTime(150)
                .setEndTime(300)
                .setEntityId("1")
                .setSeriesId("11")
                .setVideoId("111")
                .setParentContentId("630000")
                .setDetailStayTime(10)
                .setQuery("comedy")
                .setDevice("app")
                .setOsType("android")
                .setAppVersion("9.2.0")
                .setDeviceModel("huawei-mate30")
                .setDeviceBrand("huawei")
                .setOsVersion("10")
                .setBrowserType("chrome")
                .setUserAgent("Mozilla/5.0 (Linux; Android 10; TAS-AN00; HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36")
                .setNetwork("4g")
                //.putExtra("additionalProp1", "additionalVal1")
                .build();
    }
}
