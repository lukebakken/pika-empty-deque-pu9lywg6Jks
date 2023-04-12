import logging
import pika

LOGGER = logging.getLogger(__name__)


class RabbitMQ:
    def __init__(self, host, port, user, password):
        self.credentials = pika.PlainCredentials(user, password)
        self.connection = None
        self.channel = None
        self.host = host
        self.port = port

    def close(self):
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()

    def create_connection(self):
        # Establish a connection to RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=self.credentials,
                socket_timeout=50,
                heartbeat=50,
            )
        )

        return self.connection

    def reconnect(self):
        while True:
            try:
                connection = self.create_connection()
                break
            except:
                # If the connection fails, wait a short period of time before trying again
                # time.sleep(0.40)
                LOGGER.info("-----Trying to reconnect-----")
        return connection

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=self.credentials,
                    socket_timeout=50,
                    heartbeat=50,
                )
            )

            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            LOGGER.info("-----RabbitMQ Connected-----")
        except:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=self.credentials,
                    socket_timeout=50,
                    heartbeat=50,
                )
            )

            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            LOGGER.info("-----RabbitMQ Connected-----")

    def publish_to_staff(self, data, user):
        """
        data: str (message that you need to publish)
        user: list (list of exchanges, on what you want to publish message)
        """
        try:
            for user in user:
                try:
                    if self.channel.is_closed:
                        self.connection = self.reconnect()
                        self.channel = self.connection.channel()
                        self.channel.confirm_delivery()
                        LOGGER.info("-----RabbitMQ Reconnected-----")
                    self.channel.exchange_declare(
                        exchange=user, exchange_type="fanout", durable=True
                    )
                    self.channel.basic_publish(
                        exchange=user, routing_key="", body=str(data)
                    )
                    LOGGER.info(
                        f"-----Message Published From RabbitMQ Successfully-----\n{str(data)}"
                    )
                except:
                    self.connection = self.reconnect()
                    self.channel = self.connection.channel()
                    self.channel.confirm_delivery()
                    self.channel.exchange_declare(
                        exchange=user, exchange_type="fanout", durable=True
                    )
                    self.channel.basic_publish(
                        exchange=user, routing_key="", body=str(data)
                    )
                    LOGGER.info(
                        f"-----Message Published From RabbitMQ Successfully-----\n{str(data)}"
                    )
        except Exception as e:
            logger.error(
                f"Exception occurred at **** dal / rabbitmq_producer / publish_to_staff **** \n {e}",
                exc_info=True,
            )
