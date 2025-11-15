package ca.quarksys.kafkaperf;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaPerfApplication implements CommandLineRunner {

    private final BaselineRunner baselineRunner;

    public SpringKafkaPerfApplication(BaselineRunner baselineRunner) {
        this.baselineRunner = baselineRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaPerfApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        baselineRunner.execute();
    }
}

