package cc.caucas.search;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpServer;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Georgy Davityan.
 */
public class Server extends AbstractVerticle {

    private static final Map<String, JsonObject> cache = new ConcurrentHashMap<>();

    public static void main(String... args) {
        Vertx vertx = Vertx.vertx();
        vertx.fileSystem()
                .readFileObservable("src/main/resources/application-config.json")
                .flatMap(data -> Observable.just(Buffer.buffer()).mergeWith(Observable.just(data)))
                .reduce(Buffer::appendBuffer)
                .subscribe(config -> vertx.deployVerticle(Server.class.getName(),
                        new DeploymentOptions().setConfig(config.toJsonObject())));
    }

    @Override
    public void start() throws Exception {
        EventBus eb = vertx.eventBus();
        eb.consumer("search.request").toObservable()
                .subscribe(this::onSearch);

        HttpServer server = vertx.createHttpServer();
        server.requestStream().toObservable()
                .subscribe(request -> {
                    String value = request.params().get("text");
                    if (value != null) {
                        eb.sendObservable("search.request", value)
                                .subscribe(reply ->
                                        request.response()
                                                .putHeader("Content-Type", "application/json; charset=UTF-8")
                                                .end(reply.body().toString())
                                );
                    }
                });
        server.listen(config().getInteger("http.server.port"));
    }

    private void onSearch(Message<Object> message) {
        String searchKey = (String) message.body();
        findInLocalCache(searchKey).concatWith(findInNetwork(searchKey))
                .first()
                .subscribe(message::reply);
    }

    private Observable<JsonObject> findInNetwork(String key) {
        HttpClient client = vertx.createHttpClient(
                new HttpClientOptions(config().getJsonObject("http.client")));

        HttpClientRequest request;
        try {
            request = client
                    .get("/taxi/public/search?pattern=" + URLEncoder.encode(key, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Can't encode URL", e);
        }

        PublishSubject<JsonObject> subject = PublishSubject.create();
        request.toObservable()
                .flatMap(response -> Observable.just(Buffer.buffer()).mergeWith(response.toObservable()))
                .reduce(Buffer::appendBuffer)
                .doOnNext(o -> {
                    JsonObject jsonObject = o.toJsonObject();
                    cache.put(key, jsonObject);
                    subject.onNext(jsonObject);
                })
                .doOnCompleted(subject::onCompleted)
                .doOnError(subject::onError)
                .subscribe();

        request.end();

        return subject.asObservable();
    }

    private Observable<JsonObject> findInLocalCache(String key) {
        JsonObject data = cache.get(key);
        if (data != null) {
            return Observable.just(data);
        }
        return Observable.empty();
    }

}
