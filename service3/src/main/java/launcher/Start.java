package launcher;

import io.vertx.core.Vertx;

public class Start {
    public static void main(String[] args) {
        Vertx.vertx().createHttpServer().requestHandler(req -> {
            req.response()
                    .setStatusCode(200)
                    .putHeader("content-type", "text/plain")
                    .end("OK");
        }).listen(9002);
    }
}
