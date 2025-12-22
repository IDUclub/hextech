import asyncio

import pandas as pd
from loguru import logger

from app.grid_generator.services.potential_estimator import potential_estimator
from app.indicators_savior.indicators_savior_services.indicators_savior_api_service import (
    indicators_savior_api_service,
)
from app.prioc.services import prioc_service

from ..grid_generator.services.constants.constants import prioc_objects_types
from ..grid_generator.services.generator_api_service import generator_api_service
from .indicators_savior_services.indicators_constants import objects_name_id_map

# TODO remove hardcode
BASE_INDICATORS_IDS = [197, 198, 199, 200, 204]


# ToDo rewrite whole service.
class IndicatorsSaviorService:

    @staticmethod
    async def post_potentials(
        potential_ter_estimation: dict, project_scenario_id: int
    ) -> None:

        potentials_id_map = await indicators_savior_api_service.get_name_id_map(269)
        async_task_list = []
        for key in potential_ter_estimation:
            value = potential_ter_estimation[key]
            indicator_id = potentials_id_map[key]
            first_to_put = {
                "indicator_id": indicator_id,
                "scenario_id": project_scenario_id,
                "territory_id": None,
                "hexagon_id": None,
                "value": value,
                "comment": None,
                "information_source": "hextech/potential",
                "properties": {},
            }
            async_task_list.append(
                indicators_savior_api_service.put_indicator(
                    scenario_id=project_scenario_id, json_data=first_to_put
                )
            )

        await asyncio.gather(*async_task_list)

    @staticmethod
    async def post_all(
        prioc_ter_estimation: dict,
        project_scenario_id: int,
    ) -> None:

        async_task_list = []
        for key in prioc_ter_estimation:
            value = prioc_ter_estimation[key]["estimation"]
            comment = ". ".join(prioc_ter_estimation[key]["interpretation"])
            indicator_id = objects_name_id_map[key]
            first_to_put = {
                "indicator_id": indicator_id,
                "scenario_id": project_scenario_id,
                "territory_id": None,
                "hexagon_id": None,
                "value": value,
                "comment": comment,
                "information_source": "hextech/prioc",
                "properties": {},
            }
            async_task_list.append(
                indicators_savior_api_service.put_indicator(
                    scenario_id=project_scenario_id, json_data=first_to_put
                )
            )

        await asyncio.gather(*async_task_list)

    async def save_potential_and_base_indicators(
        self, scenario_id: int, indicators_values: list[dict]
    ) -> None:
        """
        Function calculates potential and saves result to db
        Args:
            scenario_id (int): id of scenario
            indicators_values (list[dict]): a list of indicators values dictionaries
        Returns:
            None
        """

        indicators_dict = {}
        for indicator in indicators_values:
            for key, value in indicator.items():
                indicators_dict[key] = value
        result_dict = await potential_estimator.estimate_potentials_as_dict(
            indicators_dict
        )
        await self.post_potentials(result_dict, scenario_id)
        logger.info("Saved all potential indicators")

    @staticmethod
    async def save_recultivation(
        area: dict,
        base_scenario_id: int,
        target_scenario_id: int,
    ) -> None:
        """
        Save all recultivation results

        Args:
            area (dict): dict with geometry
            base_scenario_id (int): id of base scenario
            target_scenario_id (int): id of target scenario
        Returns:
            None
        """

        recultivation_data = (
            await indicators_savior_api_service.get_recultivation_marks(
                area=area,
                base_scenario_id=base_scenario_id,
                target_scenario_id=target_scenario_id,
            )
        )

        time_estimation = {
            "indicator_id": 299,
            "scenario_id": target_scenario_id,
            "territory_id": None,
            "hexagon_id": None,
            "value": recultivation_data["data"]["recultivation"]["total"]["timeOfWork"],
            "comment": None,
            "information_source": "Redevelopment Generation",
            "properties": {},
        }

        money_estimation = {
            "indicator_id": 298,
            "scenario_id": target_scenario_id,
            "territory_id": None,
            "hexagon_id": None,
            "value": recultivation_data["data"]["recultivation"]["total"]["costOfWork"],
            "comment": None,
            "information_source": "Redevelopment Generation",
            "properties": {},
        }

        task_list = [
            indicators_savior_api_service.put_indicator(
                scenario_id=target_scenario_id, json_data=time_estimation
            ),
            indicators_savior_api_service.put_indicator(
                scenario_id=target_scenario_id, json_data=money_estimation
            ),
        ]
        await asyncio.gather(*task_list)
        logger.info("Saved all recultivation indicators")

    async def save_prioc_evaluations(
        self, scenario_id: int, territory_id: int, territory: dict
    ) -> None:
        """
        Function calculates parameters and saves result to db
        Args:
            scenario_id (int): id of scenario
            territory_id (int): id of region
            territory (dict): dict with territory geometry
        Returns:
            None
        """

        evaluations = await prioc_service.get_territory_estimation(
            territory=territory,
            territory_id=territory_id,
        )
        await self.post_all(evaluations, scenario_id)
        logger.info("Saved prioc evaluations")

    async def save_all_indicators(self, scenario_id: int):
        """
        Function calculates and save all indicators to db
        Args:
            scenario_id (int): id of scenario to save indicators for
        Returns:
            None
        """

        indicators_values = await indicators_savior_api_service.get_indicators_values(
            scenario_id, BASE_INDICATORS_IDS
        )
        project_id = await indicators_savior_api_service.get_project_id_by_scenario(
            scenario_id
        )
        territory_data = await indicators_savior_api_service.get_project_data(
            project_id
        )
        territory_id = territory_data["project"]["region"]["id"]

        extract_list = [
            self.save_prioc_evaluations(
                scenario_id=scenario_id,
                territory_id=territory_id,
                territory=territory_data["geometry"],
            ),
            self.save_potential_and_base_indicators(
                scenario_id=scenario_id, indicators_values=indicators_values
            ),
        ]
        await asyncio.gather(*extract_list)
        logger.info(f"Finished saving all indicators with scenario id {scenario_id}")
        return {"msg": f"Successfully saved all indicators for scenario {scenario_id}"}

    @staticmethod
    async def save_hexagonal_indicators(regional_scenario_id: int, territory_id: int):

        grid_with_indicators = (
            await indicators_savior_api_service.get_grid_with_indicators(
                regional_scenario_id, BASE_INDICATORS_IDS
            )
        )
        bounded_hexagons = await potential_estimator.estimate_potentials(
            grid_with_indicators
        )
        for i in prioc_objects_types:
            current_object_hexes = await prioc_service.get_hexes_for_object_from_gdf(
                hexes=bounded_hexagons, territory_id=territory_id, object_type=i
            )
            bounded_hexagons = pd.merge(
                bounded_hexagons,
                current_object_hexes[["hexagon_id", "weighted_sum"]],
                on="hexagon_id",
                how="outer",
            )
            if i == "Пром объект":
                bounded_hexagons.rename(
                    columns={"weighted_sum": "Промышленная зона"}, inplace=True
                )
            elif i == "Логистическо-складской комплекс":
                bounded_hexagons.rename(
                    columns={"weighted_sum": "Логистический, складской комплекс"},
                    inplace=True,
                )
            elif i == "Кампус университетский":
                bounded_hexagons.rename(
                    columns={"weighted_sum": "Университетский кампус"}, inplace=True
                )
            elif i == "Тур база":
                bounded_hexagons.rename(
                    columns={"weighted_sum": "Туристическая база"}, inplace=True
                )
            else:
                bounded_hexagons.rename(columns={"weighted_sum": i}, inplace=True)
        full_map = await generator_api_service.extract_all_indicators()
        mapped_name_id = {}
        for item in full_map:
            if item["name_full"] in bounded_hexagons.columns:
                mapped_name_id[item["name_full"]] = item["indicator_id"]
            elif item["name_short"] in bounded_hexagons.columns:
                mapped_name_id[item["name_short"]] = item["indicator_id"]
        bounded_hexagons.drop_duplicates("geometry", inplace=True)
        df_to_put = bounded_hexagons.drop(
            columns=[
                column
                for column in bounded_hexagons.columns
                if column in ["geometry", "properties"]
            ]
        )
        columns_to_iter = list(df_to_put.drop(columns="hexagon_id").columns)
        extract_list = []
        failed_list = []
        for index, row in df_to_put.iterrows():
            for column in columns_to_iter:
                if not pd.isna(row[column]):
                    extract_list.append(
                        {
                            "indicator_id": int(mapped_name_id[column]),
                            "scenario_id": regional_scenario_id,
                            "territory_id": None,
                            "hexagon_id": int(row["hexagon_id"]),
                            "value": row[column],
                            "comment": "--",
                            "information_source": "hextech/grid_generator",
                            "properties": {},
                        }
                    )

        if failed_list:
            logger.warning("Failed to upload data {}".format(failed_list))
        await generator_api_service.put_hexagon_data(extract_list, regional_scenario_id)

        return {"msg": f"Successfully uploaded hexagons data for {territory_id}"}


indicators_savior_service = IndicatorsSaviorService()
