import asyncio

import aiohttp
import geopandas as gpd
import pandas as pd
from fastapi.exceptions import HTTPException
from loguru import logger

from app.common import config
from app.common.api_handler.api_handler import (
    urban_api_handler,
)
from app.common.exceptions.http_exception_wrapper import http_exception

from .recaltivation_api_handler import recultivation_api_handler


class IndicatorsSaviorApiService:

    def __init__(self):
        self.headers = {"Authorization": f'Bearer {config.get("ACCESS_TOKEN")}'}

    async def get_base_scenario_by_project(self, project_id: int) -> int:
        """
        Function returns base scenario for project

        Args:
            project_id (int): id of project
        Returns:
            int: base scenario
        """

        base_scenario_data = await urban_api_handler.get(
            extra_url=f"/api/v1/projects/{project_id}/scenarios",
            headers=self.headers,
        )
        base_scenario_df = pd.DataFrame(base_scenario_data)
        base_scenario_id = int(
            base_scenario_df[base_scenario_df["is_based"]]["scenario_id"].iloc[0]
        )
        return base_scenario_id

    async def get_project_id_by_scenario(self, scenario_id: int) -> int:

        scenario_data = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{scenario_id}", headers=self.headers
        )
        return scenario_data["project"]["project_id"]

    async def put_indicator(self, scenario_id: int, json_data: dict) -> None:
        """
        Function extracts put indicators query with params
        Args:
            scenario_id (int): id of scenario
            json_data (dict): indicators_data_to_post
        Returns:
            None
        """

        async with aiohttp.ClientSession() as session:
            await urban_api_handler.put(
                session=session,
                extra_url=f"/api/v1/scenarios/{scenario_id}/indicators_values",
                headers=self.headers,
                data=json_data,
            )

    async def get_project_data(self, project_id: int) -> list | dict | None:
        """
        Function extracts project data for given project id

        Args:
            project_id (int): project id from urban_db
        Returns:
            dict with project data
        """

        for i in range(int(config.get("MAX_RETRIES")) + 1):
            try:
                response = await urban_api_handler.get(
                    extra_url=f"/api/v1/projects/{project_id}/territory",
                    headers=self.headers,
                )
                return response
            except HTTPException as e:
                if i < int(config.get("MAX_RETRIES")):
                    if e.status_code == 404:
                        logger.warning(
                            f"Project with id {project_id} not found, retry attempt {i+1} in  10 seconds"
                        )
                        await asyncio.sleep(10)
                        continue
                else:
                    raise e
            except Exception as e:
                logger.error(e)
                raise e

        return None

    async def get_recultivation_marks(
        self,
        area: dict,
        base_scenario_id: int,
        target_scenario_id: int,
    ) -> None:
        """
        Function calculate and get recultivation marks to urban api

        Args:
            area (dict): area geometry from urban_db
            base_scenario_id (int): base scenario id from urban_db
            target_scenario_id (int): target scenario id from urban_db
        Returns:
            None
        """

        async def _form_source_params(sources: list[dict]) -> dict | None:
            source_names = [i["source"] for i in sources]
            source_data_df = pd.DataFrame(sources)
            if "PZZ" in source_names:
                return source_data_df.loc[
                    source_data_df[source_data_df["source"] == "PZZ"]["year"].idxmax()
                ].to_dict()
            elif "OSM" in source_names:
                return source_data_df.loc[
                    source_data_df[source_data_df["source"] == "OSM"]["year"].idxmax()
                ].to_dict()
            elif "User" in source_names:
                return source_data_df.loc[
                    source_data_df[source_data_df["source"] == "User"]["year"].idxmax()
                ].to_dict()
            else:
                return None

        async def _map_matrix_names(matrix_data: dict[str, list]) -> dict[str, list]:
            renamed_recultivation_matrix = {}
            for i in list(matrix_data.keys()):
                if i != "labels":
                    name = i.split("_")
                    name[1] = name[1].capitalize()
                    name = "".join(name)
                    renamed_recultivation_matrix[name] = matrix_data[i]
                else:
                    renamed_recultivation_matrix[i] = matrix_data[i]
            return renamed_recultivation_matrix

        base_zones_source = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{base_scenario_id}/functional_zone_sources",
            headers=self.headers,
        )
        target_zones_source = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{target_scenario_id}/functional_zone_sources",
            headers=self.headers,
        )
        base_source_params = await _form_source_params(base_zones_source)
        if not base_source_params:
            logger.warning(f"No source found for base scenario with {base_scenario_id}")
            raise http_exception(
                404,
                f"No pzz source found for base scenario",
                _input=base_zones_source,
                _detail={
                    "scenario_id": base_scenario_id,
                },
            )
        target_source_params = await _form_source_params(target_zones_source)
        if not target_source_params:
            logger.warning(
                f"No source found for target scenario with {base_scenario_id}"
            )
            raise http_exception(
                404,
                f"No pzz source found for target scenario",
                _input=target_zones_source,
                _detail={"scenario_id": target_scenario_id},
            )

        base_func_zones = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{base_scenario_id}/functional_zones",
            headers=self.headers,
            params=base_source_params,
        )
        func_zones = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{target_scenario_id}/functional_zones",
            headers=self.headers,
            params=target_source_params,
        )
        base_ids = set(
            [
                i["properties"]["functional_zone_type"]["id"]
                for i in base_func_zones["features"]
            ]
        )
        target_ids = set(
            [
                i["properties"]["functional_zone_type"]["id"]
                for i in func_zones["features"]
            ]
        )
        matrix_labels = [str(i) for i in (base_ids | target_ids)]
        cost_matrix = await urban_api_handler.get(
            extra_url=f"/api/v1/profiles_reclamation/matrix?labels={','.join(matrix_labels)}",
        )
        recultivation_matrix = await _map_matrix_names(cost_matrix)

        request_json = {
            "area": {
                "geometry": area,
                "sourcePzzAreas": [
                    {
                        "allowedCode": str(
                            i["properties"]["functional_zone_type"]["id"]
                        ),
                        "geometry": i["geometry"],
                    }
                    for i in base_func_zones["features"]
                ],
                "targetPzzAreas": [
                    {
                        "allowedCode": str(
                            i["properties"]["functional_zone_type"]["id"]
                        ),
                        "geometry": i["geometry"],
                    }
                    for i in func_zones["features"]
                ],
                "recultivationTable": recultivation_matrix,
            },
            "request": {
                "recultivation": True,
                "demolition": False,
                "construction": False,
                "allowExplosiveDemolition": False,
                "utilizationIndex": {"costOfWork": 1.2, "timeOfWork": 1.2},
                "breakdownsIndex": {"costOfWork": 1.2, "timeOfWork": 1.2},
                "hazardIndex": {"costOfWork": 1.2, "timeOfWork": 1.1},
            },
            "parameters": {
                "wasteEliminationAfterDismantlement": {
                    "costOfWork": 390.0,
                    "timeOfWork": 0.0001,
                },
                "levelingAfterDismantlement": {
                    "costOfWork": 300.0,
                    "timeOfWork": 0.003,
                },
                "footingDismantlementPerUnit": {
                    "costOfWork": 1500.0,
                    "timeOfWork": 0.01,
                },
                "roofDismantlementPerUnit": {"costOfWork": 500.0, "timeOfWork": 0.015},
                "utilityDismantlementPerUnit": {
                    "costOfWork": 300.0,
                    "timeOfWork": 0.0005,
                },
                "constructionSitePerUnit": {"costOfWork": 200.0, "timeOfWork": 0.005},
                "minExplosiveDistance": 100.0,
                "minMechanicalDistance": 100.0,
                "minMechanicalHeight": 20.0,
                "explosiveDemolition": {
                    "overlappingDemolition": {
                        "costOfWork": 100.0,
                        "timeOfWork": 0.0001,
                    },
                    "wallsDemolition": {"costOfWork": 100.0, "timeOfWork": 0.0001},
                },
                "mechanicalDemolition": {
                    "overlappingDemolition": {"costOfWork": 600.0, "timeOfWork": 0.004},
                    "wallsDemolition": {"costOfWork": 600.0, "timeOfWork": 0.004},
                },
                "halfMechanicalDemolition": {
                    "overlappingDemolition": {"costOfWork": 3000.0, "timeOfWork": 0.02},
                    "wallsDemolition": {"costOfWork": 3000.0, "timeOfWork": 0.025},
                },
                "depthIndex": 1.0,
            },
        }

        response = await recultivation_api_handler.post(
            extra_url=f"/api/v1/redevelopment/calculate",
            data=request_json,
        )
        return response

    async def get_indicators_values(
        self, scenario_id: int, indicators_ids_list: list[int]
    ) -> list[dict]:

        response = await urban_api_handler.get(
            extra_url=f"/api/v1/scenarios/{scenario_id}/indicators_values",
            params={"indicators_ids": ",".join([str(i) for i in indicators_ids_list])},
            headers=self.headers,
        )
        response_indicators_ids = {
            i["indicator"]["indicator_id"]: i["value"] for i in response
        }
        for i in indicators_ids_list:
            if i not in response_indicators_ids:
                raise Exception(f"Indicator {i} not found")
        return [
            {"Население": response_indicators_ids[197]},
            {"Транспортное обеспечение": response_indicators_ids[198]},
            {"Экологическая ситуация": response_indicators_ids[199]},
            {"Социальное обеспечение": response_indicators_ids[200]},
            {"Обеспечение инженерной инфраструктурой": response_indicators_ids[204]},
        ]

    @staticmethod
    async def get_name_id_map(parent_id: int):
        """
        Function retrieves name_id map by indicator parent id

        Args:
            parent_id (int): Parent ID
        Returns:
            dict: Name ID map and extra info map
        """

        response = await urban_api_handler.get(
            extra_url="/api/v1/indicators_by_parent",
            params={"parent_id": parent_id, "get_all_subtree": "true"},
        )

        result = {}
        for i in response:
            result[i["name_full"]] = i["indicator_id"]
        return result

    async def get_grid_with_indicators(
        self, regional_scenario_id: int, indicators_list: list[int]
    ) -> gpd.GeoDataFrame:

        response = await urban_api_handler.get(
            f"/api/v1/scenarios/{regional_scenario_id}/indicators_values/hexagons",
            params={"indicator_ids": ",".join([str(i) for i in indicators_list])},
            headers=self.headers,
        )
        gdf = gpd.GeoDataFrame.from_features(response, crs=4326)
        df_data = gdf["indicators"].apply(
            lambda x: {i["name_full"]: i["value"] for i in x}
        )
        indicators_df = pd.DataFrame.from_records(df_data.to_list())
        gdf = pd.concat([gdf, indicators_df], axis=1).drop(columns="indicators")
        return gdf


indicators_savior_api_service = IndicatorsSaviorApiService()
