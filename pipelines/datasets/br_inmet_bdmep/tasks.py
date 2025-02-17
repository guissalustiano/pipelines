# -*- coding: utf-8 -*-
"""
Tasks for br_inmet_bdmep
"""
import glob
import os
from datetime import datetime

import numpy as np
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.constants import constants as inmet_constants
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    year_list,
)
from pipelines.utils.utils import log

# pylint: disable=C0103


@task
def get_base_inmet(year: int) -> str:
    """
    Faz o download dos dados meteorológicos do INMET, processa-os e salva os dataframes resultantes em arquivos CSV.

    Retorna:
    - str: o caminho para o diretório que contém os arquivos CSV de saída.
    """
    log(f"Baixando os dados para o ano {year}.")

    download_inmet(year)
    log("Dados baixados.")

    files = glob.glob(os.path.join(f"/tmp/data/input/{year}/", "*.CSV"))

    base = pd.concat([get_clima_info(file) for file in files], ignore_index=True)

    # ordena as colunas
    ordem = inmet_constants.COLUMNS_ORDER.value
    base = base[ordem]

    # Salva o dataframe resultante em um arquivo CSV
    os.makedirs(os.path.join(f"/tmp/data/output/microdados/ano={year}"), exist_ok=True)
    name = os.path.join(
        f"/tmp/data/output/microdados/ano={year}/", f"microdados_{year}.csv"
    )
    base.to_csv(name, index=False)

    return "/tmp/data/output/microdados/"


@task
def get_today_date():
    d = datetime.today()

    return d.strftime("%Y-%m")
