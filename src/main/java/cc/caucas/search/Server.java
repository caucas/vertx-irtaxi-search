package cc.caucas.search;

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
        vertx.deployVerticle(Server.class.getName());
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
        server.listen(8080);
    }

    private void onSearch(Message<Object> message) {
        String searchKey = (String) message.body();
        findInLocalCache(searchKey).concatWith(findInNetwork(searchKey))
                .first()
                .subscribe(data -> message.reply(data));
    }

    private Observable<JsonObject> findInNetwork(String key) {
        HttpClient client = vertx.createHttpClient(
                new HttpClientOptions()
                        .setDefaultHost("irtaxi.ru")
                        .setDefaultPort(80));
        HttpClientRequest request = null;
        try {
            request = client
                    .get("/taxi/public/search?pattern=" + URLEncoder.encode(key, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        PublishSubject<JsonObject> subject = PublishSubject.create();
        request.toObservable()
                .subscribe(response -> {
                    response.toObservable()
                            .reduce(Buffer.buffer(), (a, b) -> Buffer.buffer(a.toString().concat(b.toString())))
                            .asObservable()
                            .doOnNext(o -> {
                                JsonObject jsonObject = o.toJsonObject();
                                cache.put(key, jsonObject);
                                subject.onNext(jsonObject);
                            })
                            .doOnCompleted(subject::onCompleted)
                            .doOnError(subject::onError)
                            .subscribe();
                });

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
