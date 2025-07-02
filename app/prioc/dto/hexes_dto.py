from typing import Literal

from pydantic import BaseModel, Field


class HexesDTO(BaseModel):

    territory_id: int = Field(
        ..., examples=[1], description="Territory id to calculate hexes priority"
    )

    object_type: Literal[
        "Медицинский комплекс",
        "Бизнес-кластер",
        "Пром объект",
        "Логистическо-складской комплекс",
        "Кампус университетский",
        "Тур база",
    ] = Field(
        ..., examples=["Тур база"], description="Possible object to place in territory"
    )
