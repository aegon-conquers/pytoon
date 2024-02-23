import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

public class PostRequestExample {

    public static void main(String[] args) {
        // Define the URL of the API you want to call
        String apiUrl = "https://api.example.com/post";

        // Define the JSON payload you want to send in the request body
        String requestBody = "{\"key\": \"value\"}";

        // Define proxy settings
        String proxyHost = "your_proxy_host";
        int proxyPort = 8080; // Change this to your proxy port

        // Create WebClient with proxy settings
        WebClient client = WebClient.builder()
                .baseUrl(apiUrl)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()
                        .proxy(proxy -> proxy.type(Proxy.HTTP)
                                .host(proxyHost)
                                .port(proxyPort))))
                .build();

        // Make a POST request with JSON content type and the request body
        String responseBody;
        try {
            responseBody = client.post()
                    .uri(apiUrl)
                    .body(BodyInserters.fromValue(requestBody))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
        } catch (Exception e) {
            // Handle exceptions
            e.printStackTrace();
            return;
        } finally {
            // Close the WebClient instance
            client.mutate().build().getWebClient().getHttpClient().dispose();
        }

        // Print the response body
        System.out.println("Response Body: " + responseBody);
    }
}
