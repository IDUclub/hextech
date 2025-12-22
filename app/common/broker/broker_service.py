from iduconfig import Config
from otteroad import KafkaConsumerService

from .handlers import ScenarioHandler


class BrokerService:

    def __init__(
        self,
        config: Config,
        broker_client: KafkaConsumerService,
    ):

        self.config = config
        self.broker_client = broker_client

    async def register_and_start(self):

        self.broker_client.register_handler(ScenarioHandler(self.config))
        self.broker_client.add_worker(topics=["indicator.events"])

        await self.broker_client.start()

    async def stop(self):

        await self.broker_client.stop()
