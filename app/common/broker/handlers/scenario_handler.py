from iduconfig import Config
from loguru import logger
from otteroad.consumer import BaseMessageHandler
from otteroad.models import ScenarioIndicatorsUpdated

from app.common.broker.events_groups import ScenarioIndicatorsEvent
from app.indicators_savior.indicators_savior_constroller import (
    save_all_indicators_to_db,
)


class ScenarioHandler(BaseMessageHandler[ScenarioIndicatorsUpdated]):

    def __init__(
        self,
        config: Config,
    ):

        super().__init__()
        self.config = config
        self.scenarios_events: dict[int, ScenarioIndicatorsEvent] = {}
        self.indicators_processable_list = [197, 198, 199, 200, 204]

    # TODO revise ctx
    async def handle(self, event: ScenarioIndicatorsUpdated, ctx):
        """
        Function handles ScenarioIndicatorsUpdated events from broker
        Args:
            event (ScenarioIndicatorsUpdated): ScenarioIndicatorsUpdated event, should contain base_scenario attribute
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
                await save_all_indicators_to_db(scenario_id=event.scenario_id)
                logger.info(
                    f"Saved all indicators for scenario {event.scenario_id} from broker message"
                )

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        pass
