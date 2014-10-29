package reactor.rx.amqp

import reactor.core.Environment
import reactor.rx.Streams
import reactor.rx.amqp.signal.ExchangeSignal
import reactor.rx.amqp.spec.Queue
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 */
class LapinSpec extends Specification {

	@Shared
	Environment environment

	void setup() {
		environment = new Environment()
	}

	def cleanup() {
		environment.shutdown()
	}

	def 'A simple stream from rabbit queue'() {
		when:
			'we declare and listen to localhost queue'

			def latch = new CountDownLatch(1)
			def value = ''
			def value2 = ''

			LapinStreams.fromQueue(
					Queue.create('test').durable(true).bind('test')
			)
				.qosTolerance(15.0f)
				.bindAckToRequest(true)
				.dispatchOn(environment)
				.consume {
					value = it.toString()

					Streams.just(it.replyTo('hey Bernard'))
							.connect(LapinStreams.toDefaultExchange())
							.drain()

				}.finallyDo {
					println 'complete'
					value = 'test'
				}


		and:
			'a message is published'
			Streams.just('hello Bob')
					.map { ExchangeSignal.from(it) }
					.connect(LapinStreams.toExchange('test'))
					.replyTo()
					.consume {
						value2 = it.toString()
						latch.countDown()
					}

		then:
			'it is available'
			latch.await(45, TimeUnit.SECONDS)
			value == 'hello Bob'
			value2 == 'hey Bernard'
	}

}