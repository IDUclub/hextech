import asyncio

from iduconfig import Config
from otteroad import KafkaProducerClient, KafkaProducerSettings
from otteroad.models import ScenarioObjectsUpdated
from otteroad.models.indicator_events.scenarios.ScenarioIndicatorsUpdated import (
    ScenarioIndicatorsUpdated,
)
from otteroad.models.scenario_events.projects.BaseScenarioCreated import (
    BaseScenarioCreated,
)
from otteroad.models.scenario_events.regional_scenarios.RegionalScenarioCreated import (
    RegionalScenarioCreated,
)

config = Config()
producer_settings = KafkaProducerSettings.from_env()

#
# async def send_event():
#     async with KafkaProducerClient(producer_settings) as producer:
#         event = RegionalScenarioCreated(scenario_id=1855, territory_id=1)
#         await producer.send(event)


async def send_indicator_updated_event():
    async with KafkaProducerClient(producer_settings) as producer:
        for i in [197, 198, 199, 200, 204]:
            event = ScenarioIndicatorsUpdated(
                project_id=1320, scenario_id=10018, indicator_id=i, indicator_value_id=0
            )
            await producer.send(event)
        print("posted all")


asyncio.run(send_indicator_updated_event())
