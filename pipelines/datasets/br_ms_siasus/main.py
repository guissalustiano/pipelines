from dataclasses import dataclass
from enum import Enum
from functools import cache
from pathlib import Path
from tempfile import NamedTemporaryFile
import urllib.request
import os
from itertools import islice

from loguru import logger
import requests
import pandas as pd

from dbfread import DBF
from pyreaddbc import dbc2dbf

output = Path("data")

# https://stackoverflow.com/a/8991553
def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

def dbc_to_parquet(origin: Path, destination: Path, dataframe_chunk_size: int = int(1e6)):
    origin = Path(origin).as_posix()

    encoding="iso-8859-1"
    if origin.endswith(("dbc", "DBC")):
        with NamedTemporaryFile(delete=False) as tf:
            dbc2dbf(origin, tf.name.encode())
            dbf = DBF(tf.name, encoding=encoding, raw=True)
    elif origin.endswith(("DBF", "dbf")):
        dbf = DBF(filename, encoding=enconding)

    for i, batch in enumerate(batched(dbf, dataframe_chunk_size)):
        logger.info(f"Writting batch {i}")
        df = pd.DataFrame(batch)

        if destination.exists():
          # Append just work with fastparquet
          df.to_parquet(destination, engine='fastparquet', append=True, index=False)
        else:
          df.to_parquet(destination, index=False)


class Month(Enum):
    january = 1
    february = 2
    march = 3
    april = 4
    may = 5
    june = 6
    july = 7
    august = 8
    september = 9
    october = 10
    november = 11
    december = 12

    def str_number(self):
        return str(self.value).zfill(2)

class BrazilState(Enum):
    acre = 'AC'
    alagoas = 'AL'
    amapa = 'AP'
    amazonas = 'AM'
    bahia = 'BA'
    ceara = 'CE'
    distrito_federal = 'DF'
    espirito_santo = 'ES'
    goias = 'GO'
    maranhao = 'MA'
    mato_grosso = 'MT'
    mato_grosso_do_sul = 'MS'
    minas_gerais = 'MG'
    para = 'PA'
    paraiba = 'PB'
    parana = 'PR'
    pernambuco = 'PE'
    piaui = 'PI'
    rio_de_janeiro = 'RJ'
    rio_grande_do_norte = 'RN'
    rio_grande_do_sul = 'RS'
    rondonia = 'RO'
    roraima = 'RR'
    santa_catarina = 'SC'
    sao_paulo = 'SP'
    sergipe = 'SE'
    tocantins = 'TO'

    def code(self):
        return self.value

@dataclass
class DataLink:
    source: str
    modality: str
    filename: str
    html_link: str
    link: str

    @staticmethod
    def from_json(json: dict):
        return DataLink(
            source = json['fonte'],
            modality = json['modalidade'],
            filename = json['arquivo'],
            html_link = json['link'],
            link = json['endereco'],
        )
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://datasus.saude.gov.br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://datasus.saude.gov.br/transferencia-de-arquivos/',
    # 'Cookie': 'BIGipServerKF9aV/YAypHzQuCIHI5lBA=!VrS+4If5OqqTkMxH0DcMTntpfM2dhu4Uy7L0nTtD9ibYFM9kser1ZhWzvzAFSIWChiJ6+zeZNKREV7w=; TS0178494f=01f6470b2f42855d79533ebfb7b516e7508df653cf9b83b9b31fd4bd077927f6e6ffc0fc4f308bbf054dcfa9ceb277f85a2aa3b7255495158e692067863796fb1b90581086',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

cookies = {
    'BIGipServerKF9aV/YAypHzQuCIHI5lBA': '!VrS+4If5OqqTkMxH0DcMTntpfM2dhu4Uy7L0nTtD9ibYFM9kser1ZhWzvzAFSIWChiJ6+zeZNKREV7w=',
    'TS0178494f': '01f6470b2f42855d79533ebfb7b516e7508df653cf9b83b9b31fd4bd077927f6e6ffc0fc4f308bbf054dcfa9ceb277f85a2aa3b7255495158e692067863796fb1b90581086',
}

def list_data_links(
    type_: str,
    source: str,
    years: list[int],
    months:list[Month] = list(Month),
    states: list[BrazilState] = list(BrazilState),
):
    months = [month.str_number() for month in months]
    states = [state.code() for state in states]

    data = {
        'tipo_arquivo[]': type_,
        'modalidade[]': 1,
        'fonte[]': source,
        'ano[]': years,
        'mes[]': months,
        'uf[]': states,
    }

    response = requests.post(
        'https://datasus.saude.gov.br/wp-content/ftp.php', 
        cookies=cookies, headers=headers, data=data
    )

    return [DataLink.from_json(json) for json in response.json()]

def read_dbf(filename) -> pd.DataFrame:
    return read_dbc(filename, encoding="iso-8859-1")


def main():
    links = list_data_links(
        type_ = 'PA',
        source = 'SIASUS',
        years = [2010]
    )

    logger.info(f"Found {len(links)} links")

    for link in links:
        dbc = output / link.filename
        parquet = dbc.with_suffix(".parquet")

        if not dbc.exists():
            urllib.request.urlretrieve(link.link, dbc)
            logger.info(f"Downloading {link.link}")

        continue
        if not parquet.exists():
            logger.info(f"Converting {dbc} to {parquet}")
            dbc_to_parquet(dbc, parquet)

if __name__ == "__main__":
    main()
