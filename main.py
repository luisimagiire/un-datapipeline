import argparse
from multiprocessing import Pool
from typing import List

import gin
import numpy as np

import pipelines.cleaner as dc
import pipelines.data_source as ds
import pipelines.utils as sc
from pipelines.parser import Parser


def argument_parser():
    parser = argparse.ArgumentParser(description='UN Data Krunch')
    parser.add_argument('-f', '--gin_config_path', type=str, help='Gin configuration file path', required=True)
    parser.add_argument('-a', '--addition', type=str, help='Additonal procedure', required=False)
    return parser


@gin.configurable("base_pipeline")
def base(files: List[str], encoder: "pipelines.encoder.BaseEncoder"=None):
    """
    Base pipeline method to be used @pararell_processing
    :param files: file paths to be processed
    :param encoder: encoder class defined @config.gin
    """
    try:
        if not encoder:
            raise ValueError("Encoder cannot be None. PLz Specificy a encoder @gin.config!")

        # Initialization
        generator = ds.build_advertise_generator(files)
        parser = Parser()
        enc_client = encoder()

        # Source generator, clean and encode advertise
        for line in generator:
            try:
                ad = dc.clean_raw_advertise(line, parser)
                if ad:
                    enc_client.encode_advertise(ad)
            except Exception as err:
                sc.message(err)

        enc_client.save_maps()

    except Exception as erro:
        sc.message(erro)


@gin.configurable("clean_pipeline")
def clean(files: List[str], encoder: "pipelines.encoder.BaseEncoder"=None):
    """
    Data pipeline method to be used @pararell_processing when data is already clean
    So, no parser is needed.
    :param files: file paths to be processed
    :param encoder: encoder class defined @config.gin
    """
    try:
        if not encoder:
            raise ValueError("Encoder cannot be None. PLz Specificy a encoder @gin.config!")

        generator = ds.build_advertise_generator(files)
        enc_client = encoder()

        for ad in generator:
            try:
                enc_client.encode_advertise(ad)
            except Exception as err:
                sc.message(err)

        enc_client.save_maps()
    except Exception as erro:
        sc.message(erro)


@gin.configurable
def parallel_process(pipeline, dataset_path, workers: int, reducer=None):
    """
    Main method that will spawn #workers processes to process data from @dataset_path
    through @pipeline method defined.

    :param pipeline: Pipeline method to be applied
    :param dataset_path: Dataset path
    :param workers: number of process to spawn (that will be limited to the number of cores do you have available)
    :param reducer: Reducer method to aggregate workers' processed data
    """

    workers_files = [arr.tolist() for arr in np.array_split(ds.get_file_paths(folder_path=dataset_path), workers)]

    print(workers_files)

    with Pool(processes=workers) as pool:
        pool.map(pipeline, workers_files)

    # Apply reducer if available and if there was more than one worker processing data
    if reducer and workers > 1:
        red = reducer()
        red.reduce_process()


if __name__ == '__main__':

    # Parsing command line args
    sys_vars = sc.pre_loading(argument_parser)

    # Chooses the configuration file to run
    execution_file: str = sys_vars['gin_config_path']
    gin.parse_config_file(execution_file)

    # Start pararell processing based on configuration file setup
    parallel_process()
