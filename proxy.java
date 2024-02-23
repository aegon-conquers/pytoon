import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

public class PostRequestExample {

    public static void main(String[] args) {
        // Define the URL of the API you want to call
        String apiUrl = "https://api.example.com/post";

        // Define the JSON payload you want to send in the request body
        String requestBody = "{\"key\": \"value\"}";

        // Define proxy settings
        String proxyHost = "your_proxy_host";
        int proxyPort = 8080; // Change this to your proxy port

        // Create HttpClient with proxy settings
        HttpClient httpClient = HttpClient.create()
                .proxy(proxy -> proxy.type(Proxy.HTTP)
                        .host(proxyHost)
                        .port(proxyPort));

        // Configure ExchangeStrategies with HttpClient
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024)) // Example codec configuration
                .build();

        // Create WebClient with custom HttpClient and ExchangeStrategies
        WebClient client = WebClient.builder()
                .exchangeStrategies(strategies)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();

        // Make a POST request with JSON content type and the request body
        String responseBody = client.post()
                                  .uri(apiUrl)
                                  .header(HttpHeaders.CONTENT_TYPE, "application/json")
                                  .body(BodyInserters.fromValue(requestBody))
                                  .retrieve()
                                  .bodyToMono(String.class)
                                  .block();

        // Print the response body
        System.out.println("Response Body: " + responseBody);
    }
}
