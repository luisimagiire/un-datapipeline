import os
from collections import deque
from typing import List
import gin
import pipelines.utils as sc
import json


def gather_from_local_folder(folder: str):
    scraps = get_file_paths(folder)
    return scraps


def gather_from_datastorage():
    # TODO: make pipeline
    raise NotImplementedError("Method not yet implemented!")


def build_advertise_generator(files: List[str]):
    qeue = deque(files)
    total_files = len(qeue)
    process_counter = 0
    while len(qeue) > 0:
        try:
            file_name = qeue.pop()

            if "jsonl" in file_name:
                for line in open(file_name, "r", encoding="utf-8"):
                    ad = json.loads(line)
                    print(ad)
                    yield ad
            else:
                ad = sc.load_json(file_name)
                for advertise in ad:
                    yield advertise

            process_counter += 1
            sc.message("{} PROCESSED!".format(file_name))
            sc.message("Worker {}% complete!".format(round(process_counter / total_files, 2) * 100))
        except Exception as err:
            sc.message(err)
    return None


@gin.configurable(blacklist=["folder_path"])
def get_file_paths(folder_path: str, market: str = None, category: str = None) -> List[str]:
    """
    Get Scrap Files from data bkp folder.
    :param folder_path: data bkp folder (dataset/raw/opt/scraps/data_bkp/2018-09-21_data_bkp)
    :param market: "olx"/"ml"
    :param category: "celulares"
    :return: List<str> file paths
    """
    special_categories = {"ml-eletronicos": ["videogames", "tv"],
                          "ml-imoveis": ["casas", "chacaras", "flat", "galpoes",
                                         "imoveis-outros", "sitios", "terrenos"]}

    tmp_ls: List[str] = []
    date_folder: List[str] = [os.path.join(folder_path, folder) for folder in os.listdir(folder_path)]

    filter_categories = []
    if category in special_categories.keys():
        filter_categories.extend(special_categories[category])
    else:
        filter_categories.append(category)

    for folder in date_folder:
        for file in os.listdir(folder):
            if market:
                file_format = "{}".format(market)
                if category and any(["{0}-{1}".format(file_format, cat) in file for cat in filter_categories]):
                    tmp_ls.append(os.path.join(folder, file))
                elif not category and file_format in file:
                    tmp_ls.append(os.path.join(folder, file))
            else:
                tmp_ls.append(os.path.join(folder, file))

    return tmp_ls

@gin.configurable(blacklist=["file_path"])
def gather_from_single_file(file_path: str) -> List[str]:
    return [file_path]
