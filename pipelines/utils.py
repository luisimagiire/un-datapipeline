import json
import logging
import os
import pickle
import sys
from datetime import datetime
from typing import Any, Dict, List

import psutil
import yaml


def config_log(log_path: str, mode: "logging.INFO/logging.ERROR" =None) -> None:
    """
    Config logging settings.
    :param log_path: path to save log files.
    :param mode: logging level to report
    :return: None
    """
    if mode is None:
        logging.basicConfig(format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filename=log_path,
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filename=log_path,
                            level=mode)


def pre_loading(argparser):
    parser = argparser()
    parsed_args = parser.parse_args(sys.argv[1:])
    sys_variables = vars(parsed_args)
    return sys_variables


def read_config_file(config_path):
    with open(config_path, "r") as yamlconfig:
        config = yaml.load(yamlconfig)
    return config


def get_notice(perc: int, base: int=10000, msg_text: str = None) -> None:
    if perc % base == 0:
        final_msg = "{} data points processed!".format(perc)
        if msg_text:
            final_msg = "{0} {1}".format(perc, msg_text)
        message(final_msg)
        message("Time: {}".format(datetime.ctime(datetime.now())))


def get_notice_full(perc: int, process_num: str, base: int=10000) -> None:
    if perc % base == 0:
        message("{0} data points processed @ {1}!".format(perc, process_num))
        message("Time: {}".format(datetime.ctime(datetime.now())))
        get_system_status()


def message(msg, *args, **kwargs) -> None:
    print('[*] ', msg, *args, **kwargs)
    logging.info(msg)


def get_system_status() -> None:
    message("Memory usage: {} %".format(psutil.virtual_memory().percent))
    message("CPU usage: {} %".format(psutil.cpu_percent()))


def load_pickle(path: str) -> Any:
    with open(path, "rb") as input_file:
        return pickle.load(input_file)


def save_pickle(path: str, obj: Any) -> None:
    with open(path + ".pckl", 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)


def start_logging(mode):
    tmp_counter = 0

    if not os.path.isdir('logs'):
        os.mkdir('logs')

    if not os.path.isdir('logs/{}'.format(mode)):
        os.mkdir('logs/{}'.format(mode))

    log_name = 'logs/{0}/process_{1}_{2}.log'.format(mode, str(datetime.date(datetime.utcnow())), tmp_counter)
    while os.path.isfile(log_name):
        tmp_counter += 1
        log_name = 'logs/{0}/process_{1}_{2}.log'.format(mode, str(datetime.date(datetime.utcnow())), tmp_counter)

    config_log(log_path=log_name, mode=logging.DEBUG)


def check_folder_old(folder_path: str) -> str:
    if not os.path.isdir(folder_path):
        message("{} not found! Creating folder...".format(folder_path))

        # Creating intermediate folders
        tmp_path, file_path = os.path.split(folder_path)
        while len(file_path.strip()) == 0:
            tmp_path, file_path = os.path.split(tmp_path)

        while not os.path.isdir(tmp_path):
            os.mkdir(tmp_path)
            tmp_path = os.path.split(tmp_path)[0]

        os.mkdir(folder_path)
    return folder_path


def load_json(json_file: str) -> Dict[Any, Any]:
    with open(json_file, 'r', encoding='utf-8') as js:
        return json.load(js)


def save_dict_2json(json_file: str, dicio: Dict[str, Any]) -> None:
    with open(json_file, 'w', encoding='utf-8') as js:
        js.write(json.dumps(dicio))


def debug_print(obj, debug: bool):
    if debug:
        message("=========================================")
        message(obj)

    return obj


def check_folder(base):
    if not os.path.isdir(base):
        message("{} not found! Creating folder...".format(base))
        try:
            if len(base.split("/")) == 1:
                os.mkdir(base)
            else:
                tmp_path, file_path = os.path.split(base)
                while not os.path.exists(tmp_path):
                    check_folder(tmp_path)
                os.mkdir(base)
        except FileExistsError:
            message("Race condition caught on creating folder {}".format(base))
            message("Returning existing folder...")
    return base


def save_output_at(file_paths: List[str]):
    def wrapper(func):
        def save():
            results = func()
            assert len(results) == len(file_paths)
            for path, result in zip(file_paths, results):
                save_pickle(path, result)
            return results
        return save
    return wrapper


def count_number_of_lines(file_path: str) -> int:
    counter = 0
    for _ in open(file_path, "r", encoding="utf-8"):
        counter += 1
    return counter


def get_line_at_jsonl(line: int, file: str):
    """
    Loads a specific line @jsonl or return None.
    :param line: line number
    :param file: jsonl path
    :return:
    """
    if not os.path.isfile(file):
        raise FileNotFoundError("File {} does not exist!".format(file))
    elif line < 0:
        raise ValueError("Line number must be positive!")

    counter = 0

    for post in open(file, "r", encoding="utf-8"):
        if counter == line:
            return json.loads(post)
        counter += 1


def get_kth_in_category(k, jsonl, category, func_filter=None):
    counter = 0
    for line in open(jsonl, "r", encoding="utf-8"):
        ad = json.loads(line)
        if func_filter:
            if func_filter(ad) and ad["category"] == category:
                counter += 1
                if counter == k:
                    return ad
        else:
            if ad["category"] == category:
                counter += 1
                if counter == k:
                    return ad
    return None


def get_jsonl_size(jsonl: str):
    counter = 0
    for _ in open(jsonl, "r", encoding="utf-8"):
        counter += 1
    return counter
