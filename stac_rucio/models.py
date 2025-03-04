from pydantic import BaseModel, Field


class RucioStac(BaseModel):
    host: str = Field(alias="rucio:host")
    scope: str = Field(alias="rucio:scope")
    name: str = Field(alias="rucio:name")
    scheme: str = Field(alias="rucio:scheme", default="https")
    size: int = Field(alias="rucio:size")
    adler32: str = Field(alias="rucio:adler32")
