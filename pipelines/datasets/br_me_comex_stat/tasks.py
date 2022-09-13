# -*- coding: utf-8 -*-

"""
Tasks for br_me_comex_stat
"""

from asyncio import tasks
import pandas as pd
import numpy as np
import os
import requests

from glob import glob
from enum import Enum
from zipfile import ZipFile
from tqdm import tqdm


def create_paths(tables, path, ufs):
    """
    Create and partition folders
    """
    path_temps = [path, path + "input/", path + "output/"]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)

    for table in tables:
        for ano in [*range(1997, 2024)]:

            for mes in [*range(1, 13)]:

                if "municipio" in table:

                    for uf in ufs:

                        os.makedirs(
                            path + f"output/{table}/ano={ano}/mes={mes}/sigla_uf={uf}",
                            exist_ok=True,
                        )

                else:
                    os.makedirs(
                        path + f"output/{table}/ano={ano}/mes={mes}/", exist_ok=True
                    )


def download_data(path):

    """
    Crawler for br_me_comex_stat
    """
    groups = {
        "ncm": ["EXP_COMPLETA", "IMP_COMPLETA"],
        "mun": ["EXP_COMPLETA_MUN", "IMP_COMPLETA_MUN"],
    }

    for item in groups.keys():
        for group in tqdm(groups[item]):
            print(f"Baixando {item} do {group}")
            url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{item}/{group}.zip"
            r = requests.get(url, verify=False, timeout=99999999)
            open(path + "input/" + f"{group}.zip", "wb").write(r.content)


@task
def clean_br_me_comex_stat():
    """
    clean and partition table
    """

    create_paths(constants.TABLE.value, constants.PATH.value, constants.UF.value)
    download_data(constants.PATH.value)

    rename_ncm = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "CO_NCM": "id_ncm",
        "CO_UNID": "id_unidade",
        "CO_PAIS": "id_pais",
        "SG_UF_NCM": "sigla_uf_ncm",
        "CO_VIA": "id_via",
        "CO_URF": "id_urf",
        "QT_ESTAT": "quantidade_estatistica",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
        "VL_FRETE": "valor_frete",
        "VL_SEGURO": "valor_seguro",
    }

    rename_mun = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "SH4": "id_sh4",
        "CO_PAIS": "id_pais",
        "SG_UF_MUN": "sigla_uf",
        "CO_MUN": "id_municipio",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
    }

    list_zip = glob(constants.PATH.value + "input/" + "*.zip")

    for filezip in list_zip:

        with ZipFile(filezip) as z:

            with z.open(filezip.split("/")[-1][:-4] + ".csv") as f:
                df = pd.read_csv(f, sep=";")

                if "MUN" in filezip:

                    df.rename(columns=rename_mun, inplace=True)

                    condicao = [
                        ((df["sigla_uf"] == "SP") & (df["id_municipio"] < 3500000)),
                        ((df["sigla_uf"] == "MS") & (df["id_municipio"] > 5000000)),
                        ((df["sigla_uf"] == "GO") & (df["id_municipio"] > 5200000)),
                        ((df["sigla_uf"] == "DF") & (df["id_municipio"] > 5300000)),
                    ]

                    valores = [
                        df["id_municipio"] + 100000,
                        df["id_municipio"] - 200000,
                        df["id_municipio"] - 100000,
                        df["id_municipio"] - 100000,
                    ]

                    df["id_municipio"] = np.select(
                        condicao, valores, default=df["id_municipio"]
                    )

                    for table in constants.TABLE.value:

                        for ano in [*range(1997, 2023)]:

                            for mes in [*range(1, 13)]:

                                for uf in constants.UF.value:

                                    if "municipio" in table:

                                        print(f"Particionando {table}_{ano}_{mes}_{uf}")

                                        df_partition = df[df["ano"] == ano].copy()
                                        df_partition = df_partition[
                                            df_partition["mes"] == mes
                                        ]
                                        df_partition = df_partition[
                                            df_partition["sigla_uf"] == uf
                                        ]
                                        df_partition.drop(
                                            ["ano", "mes", "sigla_uf"],
                                            axis=1,
                                            inplace=True,
                                        )

                                        df_partition.to_csv(
                                            constants.PATH.value
                                            + "output/"
                                            + f"{table}/ano={ano}/mes={mes}/sigla_uf={uf}/{table}.csv",
                                            index=False,
                                            encoding="utf-8",
                                            na_rep="",
                                        )

                                        del df_partition

                else:

                    df.rename(columns=rename_ncm, inplace=True)

                    for table in constants.TABLE.value:
                        for ano in [*range(1997, 2023)]:
                            for mes in [*range(1, 13)]:

                                if "ncm" in table:
                                    print(f"Particionando {table}_{ano}_{mes}")
                                    df_partition = df[df["ano"] == ano].copy()
                                    df_partition = df_partition[
                                        df_partition["mes"] == mes
                                    ]
                                    df_partition.drop(
                                        ["ano", "mes"], axis=1, inplace=True
                                    )
                                    df_partition.to_csv(
                                        constants.PATH.value
                                        + "output/"
                                        + f"{table}/ano={ano}/mes={mes}/{table}.csv",
                                        index=False,
                                        encoding="utf-8",
                                        na_rep="",
                                    )

                                    del df_partition
                del df

    return "/tmp/br_me_comex_stat/output/"