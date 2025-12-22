from .entities import IndicatorsEnum, ScenarioIndicatorsPool


class ScenarioIndicatorsEvent:

    def __init__(self, scenario_id: int):

        self.scenario_id = scenario_id
        self.indicators: ScenarioIndicatorsPool = ScenarioIndicatorsPool()

    def check_indicators(self) -> bool:
        """
        Function checks weather all indicators are available to start event handling
        """

        return not None in self.indicators.list_indicators()

    def add_indicator(self, indicator_id: int) -> bool:

        match indicator_id:
            case IndicatorsEnum.TRANSPORT:
                self.indicators.transport_id = indicator_id
            case IndicatorsEnum.ECOLOGY:
                self.indicators.ecological_id = indicator_id
            case IndicatorsEnum.POPULATION:
                self.indicators.population_id = indicator_id
            case IndicatorsEnum.SOCIAL:
                self.indicators.social_id = indicator_id
            case IndicatorsEnum.ENGINEERING:
                self.indicators.engineering_id = indicator_id
            case _:
                raise ValueError("Unsupported indicator id {}".format(indicator_id))

        return self.check_indicators()
