import asyncio

from iduconfig import Config
from otteroad import KafkaProducerClient, KafkaProducerSettings
from otteroad.models import ScenarioObjectsUpdated
from otteroad.models.scenario_events.projects.BaseScenarioCreated import (
    BaseScenarioCreated,
)
from otteroad.models.scenario_events.regional_scenarios.RegionalScenarioCreated import (
    RegionalScenarioCreated,
)

config = Config()
producer_settings = KafkaProducerSettings.from_env()


async def send_event():
    async with KafkaProducerClient(producer_settings) as producer:
        event = RegionalScenarioCreated(scenario_id=1855, territory_id=1)
        await producer.send(event)


asyncio.run(send_event())
