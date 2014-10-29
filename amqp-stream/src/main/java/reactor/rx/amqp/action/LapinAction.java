package reactor.rx.amqp.action;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.action.Action;
import reactor.rx.amqp.Lapin;
import reactor.rx.amqp.signal.ExchangeSignal;
import reactor.rx.amqp.signal.QueueSignal;
import reactor.rx.amqp.spec.Exchange;
import reactor.rx.amqp.spec.Queue;
import reactor.rx.amqp.stream.LapinStream;
import reactor.rx.subscription.PushSubscription;

import java.io.IOException;

/**
 * @author Stephane Maldini
 */
public class LapinAction extends Action<ExchangeSignal, Void> implements ReturnListener {

	public static final String DEFAULT_ROUTING_KEY = "#";

	private final Exchange             exchangeConfig;
	private final Lapin                lapin;
	private       Channel              channel;
	private       AMQP.BasicProperties properties;

	public LapinAction(Dispatcher dispatcher, Lapin lapin, Exchange exchangeConfig) {
		super(dispatcher);
		this.lapin = lapin;
		this.exchangeConfig = exchangeConfig;
	}

	public void start() throws IOException {
		this.channel = lapin.createChannel();
		this.channel.addReturnListener(this);
		if (exchangeConfig.exchange() != null) {
			exchangeConfig.apply(this.channel);
		}
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		try {
			start();
		} catch (Exception e) {
			doError(e);
		}
	}

	public AMQP.BasicProperties properties() {
		return properties;
	}

	public LapinAction properties(AMQP.BasicProperties properties) {
		this.properties = properties;
		return this;
	}

	public LapinAction append(AMQP.BasicProperties properties) {
		this.properties = wrapProperties(properties);
		return this;
	}

	public LapinStream replyTo() throws Exception {
		return replyTo(Queue.temp());
	}

	public LapinStream replyTo(String queueName) throws Exception {
		return replyTo(queueName == null ? Queue.temp() : Queue.lookup(queueName));
	}

	public LapinStream replyTo(Queue queue) throws Exception {

		if (channel != null) {
			append(new AMQP.BasicProperties.Builder()
					.replyTo(queue.bind(channel).getQueue())
					.build());

			LapinStream lapinStream = new LapinStream(lapin, queue, false) {

				@Override
				public PushSubscription<QueueSignal> createSubscription(Subscriber<? super QueueSignal> subscriber,
				                                                        Subscription dependency) {

					final PushSubscription<QueueSignal> originalSubscription = super.createSubscription(subscriber,
							new Subscription() {
								@Override
								public void request(long elements) {
									LapinAction.this.onRequest(elements);
								}

								@Override
								public void cancel() {
									LapinAction.this.onShutdown();
								}
							});

					LapinAction.this.connect(new Action<Void, Void>() {

						@Override
						protected void doNext(Void ev) {
							//IGNORE
						}

						@Override
						protected void doComplete() {
							originalSubscription.onComplete();
						}

						@Override
						protected void doError(Throwable error) {
							originalSubscription.onError(error);
						}
					});

					return originalSubscription;
				}
			};
			return lapinStream;
		} else {
			throw new IllegalStateException("Channel is null, is the lapinAction started ?");
		}
	}

	@Override
	public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties
			properties, byte[] body) throws IOException {
		//NOT YET IMPLEMENTED
	}

	@Override
	protected void onShutdown() {
		if (this.channel != null) {
			try {
					lapin.destroyChannel(channel);
			} catch (IOException e) {
				//IGNORE
			}
		}
		super.onShutdown();
	}

	@Override
	protected void doNext(ExchangeSignal ev) {
		if (this.channel != null) {
			try {
				channel.basicPublish(
						ev.exchange() == null ? exchangeConfig.exchange() : ev.exchange(),
						ev.routingKey() == null ? DEFAULT_ROUTING_KEY : ev.routingKey(),
						ev.mandatory(),
						wrapProperties(ev.properties()),
						ev.get()
				);
			} catch (Exception e) {
				broadcastError(e);
			}
		} else {
			broadcastError(new IllegalStateException("Channel is null"));
		}
	}

	private AMQP.BasicProperties wrapProperties(AMQP.BasicProperties properties) {
		if (properties == null) {
			return this.properties;
		}
		if (this.properties == null) {
			return properties;
		}

		AMQP.BasicProperties.Builder builder = this.properties.builder();
		if (properties.getAppId() != null) {
			builder.appId(properties.getAppId());
		}
		if (properties.getClusterId() != null) {
			builder.clusterId(properties.getClusterId());
		}
		if (properties.getContentEncoding() != null) {
			builder.contentEncoding(properties.getContentEncoding());
		}
		if (properties.getContentType() != null) {
			builder.contentType(properties.getContentType());
		}
		if (properties.getCorrelationId() != null) {
			builder.correlationId(properties.getCorrelationId());
		}
		if (properties.getDeliveryMode() != null) {
			builder.deliveryMode(properties.getDeliveryMode());
		}
		if (properties.getExpiration() != null) {
			builder.expiration(properties.getExpiration());
		}
		if (properties.getHeaders() != null) {
			builder.headers(properties.getHeaders());
		}
		if (properties.getMessageId() != null) {
			builder.messageId(properties.getMessageId());
		}
		if (properties.getPriority() != null) {
			builder.priority(properties.getPriority());
		}
		if (properties.getReplyTo() != null) {
			builder.replyTo(properties.getReplyTo());
		}
		if (properties.getTimestamp() != null) {
			builder.timestamp(properties.getTimestamp());
		}
		if (properties.getType() != null) {
			builder.type(properties.getType());
		}
		if (properties.getUserId() != null) {
			builder.userId(properties.getUserId());
		}
		return builder.build();
	}

	@Override
	public String toString() {
		return super.toString() + "{lapin=" + lapin +
				", channel=" + channel + ", exchange=" + exchangeConfig + ", props=" + properties + "}";
	}
}
