from dataclasses import dataclass


@dataclass
class RegionIndicatorsPool:

    transport_indicator_value_id: int | None = None
    ecoframe_indicator_value_id: int | None = None
    population_indicator_value_id: int | None = None
    townsnet_indicator_value_id: int | None = None
