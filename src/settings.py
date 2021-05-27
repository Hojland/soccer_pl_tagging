import os
from typing import List

import pytz
from dotenv import load_dotenv
from pydantic import AnyHttpUrl, BaseSettings, HttpUrl, SecretStr
from datetime import timedelta

load_dotenv()


class Settings(BaseSettings):
    LOCAL_TZ = pytz.timezone("Europe/Copenhagen")
    ENDPOINT_PORT: int = 5000
    DATA_S3_BUCKET: str = "guardian-match-reports"
    AWS_ACCESS_KEY_ID: SecretStr = "AWS_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY: SecretStr = "AWS_SECRET_ACCESS_KEY"
    FOLDER_UPDATE_FREQ: timedelta = timedelta(days=1)


settings = Settings()
