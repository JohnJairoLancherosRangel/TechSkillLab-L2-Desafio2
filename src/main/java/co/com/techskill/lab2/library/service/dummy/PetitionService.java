package co.com.techskill.lab2.library.service.dummy;

import co.com.techskill.lab2.library.domain.dto.PetitionDTO;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PetitionService {
    private final List<PetitionDTO> petitions = new ArrayList<>();

    public PetitionService(){
        petitions.add(new PetitionDTO("09c09cc8-b", "LEND", 5, "6600ab76-3", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("2f5fca21-b", "RETURN", 7, "12a13228-0", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("4c9ef769-9", "LEND", 7, "51ed516f-a", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("5b2dae36-f", "LEND", 3, "51ed516f-a", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("ad4801f0-9", "RETURN", 5, "51ed516f-a", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("9cc825c1-7", "RETURN", 7, "12a13228-0", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("d5120259-4", "LEND", 4, "11b553eb-b", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("09ef7d35-d", "RETURN", 4, "297c17d8-4", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("0e6a31b1-f", "RETURN", 4, "6600ab76-3", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("905dfc53-7", "LEND", 5, "6600ab76-3", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("4ebc9aa6-f", "RETURN", 7, "3c24c2fa-3", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("6d7e3b2c-5", "LEND", 4, "eb25c2d4-7", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("2a6214f1-c", "RETURN", 3, "eb25c2d4-7", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("8595a9b7-7", "RETURN", 7, "51ed516f-a", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("890fd155-0", "LEND", 2, "51ed516f-a", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("2da99667-d", "LEND", 4, "1940136a-2", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("cbfdd0aa-c", "RETURN", 7, "1940136a-2", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("0ff09c9e-5", "LEND", 6, "11b553eb-b", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("86084e60-e", "RETURN", 7, "11b553eb-b", LocalDate.parse("2025-07-25")));
        petitions.add(new PetitionDTO("742330cf-0", "LEND", 6, "12a13228-0", LocalDate.parse("2025-07-25")));
    }

    public Flux<PetitionDTO> dummyFindAll(){
        return Flux.fromIterable(petitions);
    }

    public Mono<PetitionDTO> dummyFindById(String id){
        return Mono.justOrEmpty(
                petitions.stream()
                        .filter(petitionDTO -> petitionDTO.getPetitionId().equals(id))
                        .findFirst()
        );
    }

    //TO - DO: Challenge #1

    public Flux<PetitionDTO> changeDatePetitions(){

        return Flux.fromIterable(petitions)
                .map(p -> {
                    int diasAleatorios = ThreadLocalRandom.current().nextInt(1, 6); // 1 a 5
                    p.setSentAt(LocalDate.now().minusDays(diasAleatorios));;
                    p.setType("RETURN");
                    return p;
                });
    }

    public Flux<PetitionDTO> errorSimulator(){
        CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("petitionService");

        return changeDatePetitions()
                .flatMap(petition -> Mono.just(petition)
                        .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                        .flatMap(p -> {

                            System.out.println("RETURN".equalsIgnoreCase(p.getType()));
                            System.out.println(LocalDate.now());
                            System.out.println(p.getSentAt());
                            System.out.println(p.getSentAt().isBefore(LocalDate.now().minusDays(3)));

                            if ("RETURN".equalsIgnoreCase(p.getType()) &&
                                    p.getSentAt().isBefore(LocalDate.now().minusDays(3))) {
                                System.out.println("RETURN con más de 3 días de envío: " + p.getSentAt());
                                return Mono.error(new RuntimeException("RETURN con más de 3 días de envío: " + p.getSentAt()));
                            }
                            return Mono.just(p);
                        })
                        .retry(2) 
                        .timeout(Duration.ofSeconds(2))
                        .onErrorResume(error -> {
                            System.out.println("Error controlado en petición " + petition.getPetitionId() + ": " + error.getMessage());
                            return Mono.empty();
                        })
                );

    }


}
