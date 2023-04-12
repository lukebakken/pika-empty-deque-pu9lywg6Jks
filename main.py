# -*- coding: utf-8 -*-
import logging
from publish import RabbitMQ

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

rmq = RabbitMQ("localhost", 5672, "guest", "guest")

rmq.publish_to_staff("foobar", ("bazbat", "another_user"))

rmq.close()
