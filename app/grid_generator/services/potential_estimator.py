from collections import ChainMap

import geopandas as gpd
import pandas as pd
from loguru import logger

from .constants import profiles


class PotentialEstimator:

    @staticmethod
    async def estimate_potential(
        indicators_values: dict[str, int] | ChainMap,
    ) -> list[int]:
        """
        Function estimates potential for territory based on indicators

        Args:
            indicators_values: dictionary of indicators values

        Returns:
            list[int]: estimated value of potential
        """

        result = [
            sum(
                [
                    True
                    for key, value in indicators_values.items()
                    if value >= profiles[profile_name]["Критерии"][key]
                ]
            )
            for profile_name in profiles.keys()
        ]
        return result

    @staticmethod
    async def estimate_potentials_as_dict(
        indicators_values: dict,
    ):

        result = {
            profile_name: sum(
                [
                    value >= profiles[profile_name]["Критерии"][key]
                    for key, value in indicators_values.items()
                ]
            )
            for profile_name in profiles.keys()
        }

        return result

    @staticmethod
    async def estimate_potentials_weights(
        potential_min_values: dict,
    ) -> dict[str, dict[str, float]]:
        """
        Function estimates potential weights for territory based on indicators values
        Args:
            potential_min_values: dictionary of indicators values with min values
        Returns:
            dict[str, float]: estimated weight value for each potential
        """

        weights = {}
        for profile in profiles.keys():
            for indicator_name in (
                indicators_names := profiles[profile]["Критерии"].keys()
            ):
                try:
                    indicator_weight = profiles[profile]["Критерии"][
                        indicator_name
                    ] / sum(
                        [profiles[profile]["Критерии"][i] for i in indicators_names]
                    )
                    weights[profile] = {
                        indicator_name: {
                            "weight": indicator_weight,
                            "min_value": potential_min_values[profile]["Критерии"][
                                indicator_name
                            ],
                        }
                    }
                except Exception as e:
                    logger.exception(e)

        return weights

    async def estimate_potentials(self, hexes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        logger.info(f"Started potential estimation with {len(hexes)} hexes")
        result_list = []
        drop_list = [
            column
            for column in ["geometry", "hexagon_id", "properties"]
            if column in list(hexes.columns)
        ]
        hexes_df = hexes.drop(columns=drop_list)
        for index, row in hexes_df.iterrows():
            cur_potential = await self.estimate_potentials_as_dict(row.to_dict())
            result_list.append(cur_potential)

        columns = list(profiles.keys())
        hexes[columns] = pd.DataFrame.from_records(result_list)
        logger.info(f"Finished potential estimation with {len(hexes)} hexes")

        return hexes


potential_estimator = PotentialEstimator()
