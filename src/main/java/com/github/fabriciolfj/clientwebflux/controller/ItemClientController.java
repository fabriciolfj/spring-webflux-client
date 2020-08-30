package com.github.fabriciolfj.clientwebflux.controller;

import com.github.fabriciolfj.clientwebflux.domain.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Slf4j
@RestController
public class ItemClientController {

    private WebClient webClient = WebClient.create("http://localhost:8080");
    private Scheduler scheduler = Schedulers.newBoundedElastic(5, 10, "MyThreadGroup");

    /*
     * retorna direto o corpo da requisição
     * */
    @GetMapping("/client/retrieve")
    public Flux<Item> getAllItemsUsingRetrieve() {
        return webClient.get().uri("/v2/items")
                .retrieve()
                .bodyToFlux(Item.class)
                .log("Item in client project");
    }

    /*
     * Me retorna a requisição, corpo headers e etc
     * */
    @GetMapping("/client/exchange")
    public Flux<Item> getAllItemsUsingExchange() {
        return webClient.get().uri("/v2/items")
                .exchange()
                .flatMapMany(clientResponse -> clientResponse.bodyToFlux(Item.class))
                .log("Item in client project exchange");
    }

    @GetMapping("/client/retrieve/singleItem")
    public Mono<Item> getSingleData() {
        String id = "ABC";
        return webClient.get().uri("/v2/items/" + id)
                .retrieve()
                .bodyToMono(Item.class)
                .log("Mono item");

    }

    @GetMapping("/client/retrieve/singleItemThread")
    public Mono<Item> getSingleDataThread() {
        String id = "ABC";
        return webClient.get().uri("/v2/items/" + id)
                .retrieve()
                .bodyToMono(Item.class)
                .subscribeOn(scheduler) //altera o contexto da fonte de emissão
                .publishOn(scheduler) //alternat o contexto de execução
                .doOnNext(s -> log.info("Response: " + s))
                .log("Mono item");

    }

    @GetMapping("/client/exchange/singleItem")
    public Mono<Item> getSingleDataExchange() {
        String id = "ABC";
        return webClient.get().uri("/v2/items/" + id)
                .exchange()
                .flatMap(clientResponse -> clientResponse.bodyToMono(Item.class))
                .log("Mono item exchange");
    }

    @PostMapping("/client/createItem")
    public Mono<Item> createItem(@RequestBody Item item) {
        Mono<Item> body = Mono.just(item);
        return webClient.post().uri("/v2/items")
                .contentType(MediaType.APPLICATION_JSON)
                .body(body, Item.class)
                .retrieve()
                .bodyToMono(Item.class)
                .log("Post");
    }

    @PutMapping("/client/updateItem")
    public Mono<Item> updateItem(@RequestBody Item item) {
        String id = "ABC";
        Mono<Item> body = Mono.just(item);
        return webClient.put().uri("/v2/items/" + id)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body, Item.class)
                .retrieve()
                .bodyToMono(Item.class)
                .log("Post");
    }

    @DeleteMapping("/client")
    public Mono<Void> delete() {
        String id = "ABC";
        return webClient.delete().uri("v2/items/" + id)
                .retrieve()
                .bodyToMono(Void.class);
    }

    @GetMapping("/client/error")
    public Flux<Item> errorRetrieve() {
        return webClient.get()
                .uri("/v2/error")
                .retrieve()
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                    Mono<String> errorMono = clientResponse.bodyToMono(String.class);
                    return errorMono.flatMap(error -> {
                        log.error("The error message is: " + error);
                        throw new RuntimeException(error);
                    });
                })
                .bodyToFlux(Item.class);
    }

    @GetMapping("/client/error2")
    public Flux<Item> errorExchange() {
        return webClient.get()
                .uri("/v2/error")
                .exchange()
                .flatMapMany(clientResponse -> {
                    if (clientResponse.statusCode().is5xxServerError()) {
                        return clientResponse.bodyToMono(String.class)
                                .flatMap(errorMessage -> {
                                    log.error("Error message in exchange: " + errorMessage);
                                    throw new RuntimeException(errorMessage);
                                });
                    } else {
                        return clientResponse.bodyToFlux(Item.class);
                    }
                });
    }

}
