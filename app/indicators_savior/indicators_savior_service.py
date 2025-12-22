import asyncio

from loguru import logger

from app.grid_generator.services.potential_estimator import potential_estimator
from app.indicators_savior.indicators_savior_services.indicators_savior_api_service import (
    indicators_savior_api_service,
)
from app.prioc.services import prioc_service

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


indicators_savior_service = IndicatorsSaviorService()
