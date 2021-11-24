package ru.job4j.weather;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Comparator;

@RestController
public class WeatherControl {

    private final WeatherService weathers;

    public WeatherControl(WeatherService weathers) {
        this.weathers = weathers;
    }

    @GetMapping(value = "/all", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Weather> all() {
        Flux<Weather> data = weathers.all();
        Flux<Long> delay = Flux.interval(Duration.ofSeconds(3));
        return Flux.zip(data, delay).map(Tuple2::getT1);
    }

    @GetMapping(value = "/get/{id}")
    public Mono<Weather> get(@PathVariable Integer id) {
        return weathers.findById(id);
    }

    @GetMapping(value = "/hottest")
    public Mono<Weather> getHottest() {
        Flux<Weather> data = weathers.all();
        Mono<Weather> hottest = data.sort(Comparator.comparingInt(Weather::getTemperature)).last();
        return hottest;
    }

    @GetMapping(value = "/cityGreatThen/{tmp}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Weather> getGreatThen(@PathVariable Integer tmp) {
        Flux<Weather> data = weathers.all().filter(city -> city.getTemperature() > tmp);
        Flux<Long> delay = Flux.interval(Duration.ofSeconds(3));
        return Flux.zip(data, delay).map(Tuple2::getT1);
    }
}
