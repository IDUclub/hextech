from fastapi import APIRouter

from .indicators_savior_service import indicators_savior_service
from .shema import SaveResponse

indicators_savior_router = APIRouter(
    prefix="/indicators_saving", tags=["Save all indicators to db"]
)


@indicators_savior_router.put("/save_all")
async def save_all_indicators_to_db(
    scenario_id: int,
) -> SaveResponse:
    """
    Count all indicators and save them to db.
    """

    return await indicators_savior_service.save_all_indicators(scenario_id)


@indicators_savior_router.put("/save_regional_scenario")
async def save_regional_scenario_to_db(regional_scenario_id: int, territory_id: int):

    return await indicators_savior_service.save_hexagonal_indicators(
        regional_scenario_id, territory_id
    )
