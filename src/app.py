import json
import sys
from pathlib import Path

from cachetools import LRUCache, cached
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from typing import Union, List

from models.soccer_text_model import SoccerTagger, SoccerArticles
from settings import settings

app = FastAPI()
cache = LRUCache(maxsize=4)


class TextInput(BaseModel):
    text: str = Field(None, title="The text to anynomize")


class TextOutput(BaseModel):
    text: str = Field(None, title="The anonymized text")


class ActionInput(BaseModel):
    text: str = Field(None, title="The text make action on")
    action: str = Field("redact", title="'redact' or 'extract' named entities")


class NerInput(BaseModel):
    text: str = Field(None, title="The text make action on")
    action: str = Field("redact", title="'redact' or 'extract' named entities")
    entities: List[str] = Field(["PER", "NORP", "GPE", "LOC", "ORG"], title="Entities to perform action on")


class ActionOutput(BaseModel):
    out: Union[List[str], str] = Field(None, title="The redacted text or extracted entities")


class HealthResponse(BaseModel):
    ready: bool


@app.get("/update")
async def update():

    # Fetch model
    soccer_tagger = get_tagger()

    soccer_tagger.forward()

    # Return prediction result
    return 200


@app.post("/players")
async def players(teams: List):

    # Fetch model
    soccer_articles = get_soccer_articles()

    # Get from data
    out = soccer_articles.players(player)

    # Return prediction result
    res = TextOutput(text=out)
    return res


@app.get("/player_mentions")
async def player_mentions(player: str):

    # Fetch model
    soccer_articles = get_soccer_articles()

    # Get from data
    out = soccer_articles.player_mentions(player)

    # Return prediction result
    res = TextOutput(text=out)
    return res


@app.get("/health")
async def get_health(response: Response):
    status = await get_health_info()
    response.status_code = 200 if status.ready else 503
    return status


@app.get("/clear_cache")
async def clear_cache():
    cache.clear()
    return True


async def get_health_info() -> HealthResponse:
    # do checks
    ready = True
    return HealthResponse(ready=ready)


@cached(cache=cache)
def get_tagger():
    tagger = SoccerTagger()
    return tagger


@cached(cache=cache)
def get_soccer_articles():
    soccer_articles = SoccerArticles()
    return soccer_articles


#  convert to using MongoDB next!!

# GET players in team or teams, to further search
# Search on team or player to get either all adjectives in a count
# should be able to match on specific game
# get game details (so ADJ, sent and players ...  by home team, away_team and date)
# split all on game date
# get average sentiment for team or player (and conditioned on time)
# also consider VAR still
