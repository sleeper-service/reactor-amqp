package reactor.rx.amqp.subscription;

import com.rabbitmq.client.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Stream;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.rx.amqp.signal.QueueSignal;
import reactor.rx.amqp.spec.Queue;
import reactor.rx.subscription.PushSubscription;

import java.io.IOException;
import java.util.Map;

/**
 * @author Stephane Maldini
 */
public class LapinQueueSubscription extends PushSubscription<QueueSignal> implements Consumer {

	protected final boolean             bindAckToRequest;
	protected final Map<String, Object> consumerArguments;
	protected final Subscription        dependency;

	protected Channel channel;
	protected Queue   queueConfig;
	protected long lastRequest = 0l;

	private String consumerTag;
	private long   deliveryTag;


	public LapinQueueSubscription(Stream<QueueSignal> lapinStream, Subscriber<? super QueueSignal> subscriber,
	                              Queue queue,
	                              boolean bindAckToRequest,
	                              Map<String, Object> consumerArguments,
	                              Subscription dependency
	) {
		super(lapinStream, subscriber);
		this.queueConfig = queue;
		this.bindAckToRequest = bindAckToRequest;
		this.consumerArguments = consumerArguments;
		this.dependency = dependency;
	}

	protected void preRequest(long elements) {
		if (elements <= 0) throw SpecificationExceptions.spec_3_09_exception(elements);

		if (this.consumerTag == null && this.channel != null) {
			try {
				queueConfig.bind(this.channel);
				this.consumerTag = channel.basicConsume(
						queueConfig.queue(),
						!bindAckToRequest,
						consumerArguments,
						this
				);
			} catch (Exception e) {
				subscriber.onError(e);
			}
		}
	}

	@Override
	protected void onRequest(long elements) {
		preRequest(elements);

		if (channel != null && bindAckToRequest) {
			try {
				channel.basicAck(deliveryTag, true);
			} catch (IOException e) {
				onError(e);
			}
		}

		if (dependency != null) {
			dependency.request(elements);
		}
	}

	@Override
	public void cancel() {
		if (channel != null) {
			try {
				channel.basicCancel(consumerTag);
			} catch (IOException e) {
				//IGNORE
			}
		}

		super.cancel();

		if (dependency != null) {
			dependency.cancel();
		}

	}

	@Override
	public void handleConsumeOk(String consumerTag) {

	}

	@Override
	public void handleCancelOk(String consumerTag) {
		//already cleaned
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
		subscriber.onComplete();
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		subscriber.onError(sig);
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
		//Not yet implemented
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
	                           byte[] body) throws IOException {

		if (bindAckToRequest) {
			this.deliveryTag = envelope.getDeliveryTag();
		}

		subscriber.onNext(QueueSignal.from(body, consumerTag, envelope, properties));
	}

	public void channel(Channel channel) {
		this.channel = channel;
	}
}
