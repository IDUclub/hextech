from fastapi import HTTPException
from iduconfig import Config
from loguru import logger
from otteroad.consumer import BaseMessageHandler
from otteroad.models import RegionalScenarioIndicatorsUpdated

from app.common.broker.events_groups import ScenarioIndicatorsEvent
from app.indicators_savior.indicators_savior_constroller import (
    save_regional_scenario_to_db,
)


class RegionalScenarioHandler(BaseMessageHandler[RegionalScenarioIndicatorsUpdated]):

    def __init__(
        self,
        config: Config,
    ):

        super().__init__()
        self.config = config
        self.scenarios_events: dict[int, ScenarioIndicatorsEvent] = {}
        self.indicators_processable_list = [197, 198, 199, 200, 204]

    # TODO revise ctx
    async def handle(self, event: RegionalScenarioIndicatorsUpdated, ctx):
        """
        Function handles RegionalScenarioIndicatorsUpdated events from broker
        Args:
            event (RegionalScenarioIndicatorsUpdated): RegionalScenarioIndicatorsUpdated event, should contain base_scenario attribute
            ctx: Any additional context (not used here)
        Returns:
            None
        """

        logger.info("Started processing event {}", repr(event))
        if event.scenario_id not in self.scenarios_events:
            self.scenarios_events[event.scenario_id] = ScenarioIndicatorsEvent(
                scenario_id=event.scenario_id
            )
        if event.indicator_id in self.indicators_processable_list:
            print(repr(event))
            if self.scenarios_events[event.scenario_id].add_indicator(
                event.indicator_id
            ):
                print(repr(self.scenarios_events[event.scenario_id]))
                try:
                    await save_regional_scenario_to_db(
                        regional_scenario_id=event.scenario_id,
                        territory_id=event.territory_id,
                    )
                    logger.info(
                        f"Saved all indicators for scenario {event.scenario_id} from broker message"
                    )
                except HTTPException as http_e:
                    if http_e.status_code == 404:
                        logger.info(
                            "Scenario with id {} is already deleted or never existed. It won't be processed".format(
                                event.scenario_id
                            )
                        )
                    else:
                        raise http_e
                except Exception as e:
                    logger.exception(e)
                    logger.error(
                        "Error during scenario handling. Scenario id {}".format(
                            event.scenario_id
                        )
                    )

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        pass
