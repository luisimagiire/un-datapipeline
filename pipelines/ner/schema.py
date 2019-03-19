import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Set

import gin
import numpy as np

from pipelines import utils as sc
from pipelines.encoder import BaseEncoder
from pipelines.text_processors import clean_text
from pipelines.reducer import Reducer

@gin.configurable
class SchemaEncoder(BaseEncoder):
    def __init__(self, model_folder: str = None, debug: bool = False):
        super().__init__()
        self.model_folder = sc.check_folder(os.path.join(model_folder, str(datetime.date(datetime.utcnow()))))
        self.model_folder = sc.check_folder(os.path.join(self.model_folder, self.id))
        self.preload_maps()
        self.advertise_counter = 0
        self.debug = debug

    def encode_advertise(self, advertise):
        self.update_schema_dist(advertise)
        self.advertise_counter += 1
        sc.get_notice(self.advertise_counter, 5000, msg_text="ads processed!")

    def save_maps(self, *maps):
        sc.message("Saving Maps...")
        if self.model_folder:
            for k, map in self.maps.items():
                sc.save_dict_2json(os.path.join(self.model_folder, "{}.json".format(k)), map)
        else:
            # TODO: implement saving in DataStorage
            raise NotImplementedError("Saving outside a local folder path is not implemented yet!")

    def save_encoded_data(self, *data):
        if self.model_folder:
            raise NotImplementedError("Nothing to be saved")
        else:
            raise NotImplementedError("Saving outside a local folder path is not implemented yet!")

    def preload_maps(self, folder: str= None):
        if not folder:
            self.maps = {"general_schema": dict(), "schema_counter": dict()}
        else:
            raise NotImplementedError("Cannot load preloaded schemas...")

    def update_schema_dist(self, advertise):
        general_schema = self.maps["general_schema"]
        schema_counter = self.maps["schema_counter"]

        if "product_full_attributes" in advertise.keys():
            for prop in advertise["product_full_attributes"].keys():
                if prop in general_schema.keys():
                    general_schema[prop].append(advertise["product_full_attributes"][prop])
                    schema_counter[prop] += 1
                else:
                    general_schema[prop] = [advertise["product_full_attributes"][prop]]
                    schema_counter[prop] = 1

        self.maps["general_schema"] = general_schema
        self.maps["schema_counter"] = schema_counter


@gin.configurable
class SchemaReducer(Reducer):
    def __init__(self, main_folder: str, output_folder: str, debug: bool=False):
        super().__init__(main_folder, output_folder, debug)
        self.unified_general = dict()
        self.unified_counter = dict()
        self.general_counter = dict()
        self.reg_rules = [re.compile(r'[^\w\s]'), re.compile('\([^)]*\)')]

    def reduce_process(self):
        workers_folder = [os.path.join(self.main_folder, folder) for folder in os.listdir(self.main_folder)]

        for folder in workers_folder:
            g_schema = sc.load_json(os.path.join(folder, "general_schema.json"))
            c_schema = sc.load_json(os.path.join(folder, "schema_counter.json"))

            for k,v in g_schema.items():
                if k in self.unified_general.keys():
                    self.unified_general[k].extend(v)
                else:
                    self.unified_general[k] = v

            for k,v in c_schema.items():
                self.unified_counter[k] = self.unified_counter[k] + v if k in self.unified_counter.keys() else v


        self.get_general_dist()
        self.save_ner_schema()


    def get_general_dist(self):
        """
        To be executed after full general_schema is built.
        :return:
        """
        general_counter = dict()
        for k, lsval in self.unified_general.items():
            counter = dict()
            for val in lsval:
                if val in counter.keys():
                    counter[val] += 1
                else:
                    counter[val] = 1
            general_counter[k] = counter

        self.general_counter = general_counter


    def cutoff_schema(self, schema_counter: Dict[str, int], ctoff: int) -> List[str]:
        tmp_vec = []
        for k, v in schema_counter.items():
            if v > ctoff:
                tmp_vec.append(k)
            elif self.debug:
                sc.message("{} field dropped !".format(k))

        return tmp_vec

    @staticmethod
    def filter_schema(ner_schema, filtered_keys: List[str]):
        return {k:v for k,v in ner_schema if k in filtered_keys}

    def format_ner_schema(self, ner_schema: Dict[str, Any], tol=90) -> Dict[str, Any]:
        """
        Auxiliary function for @generate_apriori_ner_maps method.
        :param ner_schema:
        :return:
        """
        tmp_dict = dict()

        for key, value in ner_schema.items():
            formatted_key = clean_text(key).upper()
            formatted_key = "_".join(formatted_key.split())
            ls_tags = set([self.filter_tags(i) for i in self.filter_by_percentile(value, tol_percentile=tol)
                           if not self.filter_tags(i).isdigit()])

            if len(ls_tags) > 0:
                tmp_dict[formatted_key] = ls_tags

        return tmp_dict

    def filter_tags(self, targ):
        clean_tag = targ
        for rule in self.reg_rules:
            clean_tag = rule.sub('', clean_tag)
        return clean_tag.strip().lower()

    @staticmethod
    def filter_by_percentile(tags_distribution: Dict[str, int], tol_percentile: int) -> Set[str]:
        """
        Only returns tags that are over the 'tol_percentile' factor.
        :param tags_distribution: Dict of tags and count values
        :param tol_percentile: percentile to cut-off
        :return: set of tags
        """
        if len(list(tags_distribution.values())) > 100:
            top_percentile = np.percentile(list(tags_distribution.values()), tol_percentile)
            return {k for k, v in tags_distribution.items() if v > top_percentile}
        else:
            return {k for k, v in tags_distribution.items()}

    def save_ner_schema(self):
        if self.general_counter:
            final = {k: list(val) for k, val in self.general_counter.items()}
            final["NUMBER_OF_PROPERTIES"] = len(list(final.keys()))
            sc.save_dict_2json(os.path.join(self.output_folder, 'parsed_properties.json'), final)
        else:
            raise Exception("No general_counter map found @maps prop...")
