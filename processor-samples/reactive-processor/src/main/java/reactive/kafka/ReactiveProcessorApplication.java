package reactive.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@EnableBinding(Processor.class)
public class ReactiveProcessorApplication {

	private static final Logger LOG = LoggerFactory.getLogger(ReactiveProcessorApplication.class);

	private static final double AGGREGATE_ERROR_PROP = 0.2d;
	private static final double INPUT_ERROR_PROP = 0.0d;
	private static final double OUTPUT_ERROR_PROP = 0.0d;

	private static Random RANDOM = new Random(2728726262L);

	public static void main(String[] args) {
		SpringApplication.run(ReactiveProcessorApplication.class, args);
	}

	@StreamListener
	@Output(Processor.OUTPUT)
	public Flux<String> aggregate(@Input(Processor.INPUT) Flux<String> inbound) {
		return inbound.
				log()
				.window(Duration.ofSeconds(5), Duration.ofSeconds(5))
				.flatMap(w -> {
					boolean simulateError = RANDOM.nextDouble() < AGGREGATE_ERROR_PROP;
					if (simulateError) {
						String msg = "Error: Aggregate on " + w;
						LOG.warn(msg);
						return w.error(new IllegalStateException(msg));
					}
					return w.reduce("", (s1,s2)->s1+s2);
				})
				.log();
	}

	//Following source and sinks are used for testing only.
	//Test source will send data to the same destination where the processor receives data
	//Test sink will consume data from the same destination where the processor produces data

	@EnableBinding(Source.class)
	static class TestSource {

		private AtomicBoolean semaphore = new AtomicBoolean(true);

		private AtomicInteger count = new AtomicInteger(0);

		@Bean
		@InboundChannelAdapter(channel = "test-source", poller = @Poller(fixedDelay = "1000"))
		public MessageSource<String> sendTestData() {
			return () -> {
				boolean simulateError = RANDOM.nextDouble() < INPUT_ERROR_PROP;
				if (simulateError) {
					String msg = "SendTestData on " + count.get();
					LOG.warn(msg);
					throw new IllegalStateException(msg);
				}
				StringBuilder sb = new StringBuilder();
				sb.append(this.semaphore.getAndSet(!this.semaphore.get()) ? "foo" : "bar")
						.append(count.getAndIncrement())
						.append(", ");
				return new GenericMessage<>(sb.toString());
			};
		}
	}

	@EnableBinding(Sink.class)
	static class TestSink {

		@StreamListener("test-sink")
		public void receive(String payload) {
			boolean simulateError = RANDOM.nextDouble() < OUTPUT_ERROR_PROP;
			LOG.info("Error: " + simulateError + ", Data received: " + payload);
			if (simulateError) {
				throw new IllegalStateException("Receive on " + payload);
			}
		}
	}

	public static interface Sink {
		@Input("test-sink")
		SubscribableChannel sampleSink();
	}

	public static interface Source {
		@Output("test-source")
		MessageChannel sampleSource();
	}
}
