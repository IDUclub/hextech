from dataclasses import dataclass


@dataclass
class ScenarioIndicatorsPool:

    transport_id: int | None = None
    ecological_id: int | None = None
    population_id: int | None = None
    social_id: int | None = None
    engineering_id: int | None = None

    def list_indicators(self) -> list[int | None]:

        return list(self.__dict__.values())
