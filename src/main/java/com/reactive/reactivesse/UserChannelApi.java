package com.reactive.reactivesse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.atomic.AtomicInteger;

@CrossOrigin(origins = "*")
@RestController
public class UserChannelApi {
    private static Logger logger = LoggerFactory.getLogger(UserChannelApi.class);

    private UserChannels channels = new UserChannels();
    private AtomicInteger id = new AtomicInteger();

    @GetMapping("/notification")
    public Flux<ServerSentEvent<String>> connect() {
        Flux<String> userStream = channels.connect().toFlux();

        return userStream
                .map(str -> ServerSentEvent.builder(str).build());
    }

    @PostMapping(path = "/notification", consumes = MediaType.TEXT_PLAIN_VALUE)
    public void post(@RequestBody String message) {
        channels.post(message);
    }

    public static class UserChannels {
        private final UserChannel channel = new UserChannel();

        public UserChannel connect() {
            return channel;
        }

        public void post(String message) {
             channel.send(message);
        }
    }

    public static class UserChannel {
        private EmitterProcessor<String> processor;
        private Flux<String> flux;
        private FluxSink<String> sink;
        private Runnable closeCallback;

        public UserChannel() {
            processor = EmitterProcessor.create();
            this.sink = processor.sink();
            this.flux = processor
                    .doOnCancel(() -> {
                        logger.info("doOnCancel, downstream " + processor.downstreamCount());
                        if (processor.downstreamCount() == 0) close();
                    })
                    .doOnTerminate(() -> {
                        logger.info("doOnTerminate, downstream " + processor.downstreamCount());
                    });
        }

        public void send(String message) {
            sink.next(message);
        }

        public Flux<String> toFlux() {
            return flux;
        }

        private void close() {
            if (closeCallback != null) closeCallback.run();
            sink.complete();
        }

        public UserChannel onClose(Runnable closeCallback) {
            this.closeCallback = closeCallback;
            return this;
        }
    }
}