from loguru import logger

from app.common import config, http_exception, tasks_api_handler, urban_api_handler
from app.common.api_handler.api_handler import (
    eco_frame_api_handler,
    pop_frame_api_handler,
    townsnet_api_handler,
    transport_frame_api_handler,
)


class GeneratorApiService:
    """Class for retrieving data from urban api for grid generation"""

    def __init__(self):
        """
        Initialization function
        """

        self.headers = {"Authorization": f'Bearer {config.get("ACCESS_TOKEN")}'}
        self.territory = "/api/v1/territory"
        self.physical = "/api/v1/physical_objects"
        self.indicators = "/api/v1/indicators_by_parent"
        self.scenarios = "/api/v1/scenarios"
        self.urban_extractor = urban_api_handler
        self.townsnet_extractor = townsnet_api_handler
        self.transport_frame_extractor = transport_frame_api_handler
        self.pop_frame_extractor = pop_frame_api_handler
        self.eco_frame_extractor = eco_frame_api_handler
        self.max_async_extractions = int(config.get("MAX_API_ASYNC_EXTRACTIONS"))

    async def get_territory_data(self, territory_id: int) -> dict | list:
        """
        Function retrieves territory data

        Args:
            territory_id (int): Territory ID

        Returns:
            dict: Territory data
        """

        response = await self.urban_extractor.get(
            extra_url=f"{self.territory}/{territory_id}", params={}
        )
        return response

    async def get_intersecting_geometry(
        self, territory_geometry: dict, physical_object_id: int
    ) -> dict | list:
        """
        Function retrieves intersecting physical objects geometries with provided territory geometry

        Args:
            territory_geometry (dict): Territory geometry
            physical_object_id (int): Physical object ID

        Return:
            dict | list: Intersecting physical objects geometries
        """

        url = f"{self.physical}/around"
        response = await self.urban_extractor.post(
            extra_url=url,
            params={"physical_object_type_id": physical_object_id},
            data=territory_geometry,
        )
        return response

    async def get_hexes_from_db(self, territory_id: int) -> dict | list:
        """
        Function retrieves hexes from db

        Returns:
            dict | list: Hexes
        """

        response = await self.urban_extractor.get(
            extra_url=f"{self.territory}/{territory_id}/hexagons",
            params={"centers_only": "false"},
        )
        return response

    async def post_hexes_to_db(self, territory_id: int, json_data: dict | list) -> None:
        """
        Function posts hexes to db
        """

        await self.urban_extractor.post(
            extra_url=f"{self.territory}/{territory_id}/hexagons", data=json_data
        )

    async def delete_old_hexes_from_db(self, territory_id: int) -> None:

        await self.urban_extractor.delete(
            extra_url=f"{self.territory}/{territory_id}/hexagons"
        )

    async def extract_all_indicators(self) -> dict | list:

        result = await self.urban_extractor.get(
            extra_url=self.indicators, params={"get_all_subtree": "true"}
        )
        return result

    async def put_hexagon_data(self, data_list: list[dict], scenario_id: int) -> None:

        extra_url = f"{self.scenarios}/{scenario_id}/indicators_values"
        list_to_extract = [
            {"extra_url": extra_url, "data": hex_data} for hex_data in data_list
        ]
        for i in range(0, len(list_to_extract), self.max_async_extractions):
            chunk = list_to_extract[i : i + self.max_async_extractions]
            await tasks_api_handler.extract_requests_to_one_url(
                func=urban_api_handler.put,
                headers=self.headers,
                data=chunk,
                max_concurrent_requests=self.max_async_extractions,
            )

    async def get_regional_base_scenario(self, territory_id: int) -> dict | list:

        response = await self.urban_extractor.get(
            extra_url=f"/api/v1/scenarios",
            params={
                "territory_id": territory_id,
                "is_based": "true",
            },
        )
        if response:
            try:
                return response[0]["scenario_id"]
            except Exception as e:
                raise http_exception(
                    500,
                    "Error during extracting scenario id",
                    _input=response,
                    _detail={"error": e.__str__()},
                )
        raise http_exception(
            status_code=404,
            msg="No regional base scenario found",
            _input=territory_id,
            _detail={"available_scenario_info": response},
        )


generator_api_service = GeneratorApiService()
