
from pipelines.encoder import BaseEncoder
from pipelines.reducer import Reducer
import os
import re
from typing import Dict,Set, Optional,List, Any
import pipelines.utils as sc
import random
from datetime import datetime
import json
import gin
from pprint import pprint


@gin.configurable
class SequenceEncoder(BaseEncoder):
    def __init__(self, output_folder: str, properties_path: str, measure_exceptions: List[str],  debug: bool = False):
        super().__init__(debug=debug)
        self.output_folder = sc.check_folder(os.path.join(output_folder, str(datetime.date(datetime.utcnow()))))
        self.output_folder = sc.check_folder(os.path.join(self.output_folder, self.id))
        self.ner_dict: Dict[str, Set[str]] = self.preload_maps(folder=properties_path)
        self.reg_rules = self.build_regex()
        self.measure_map = self.generate_measure_map(self.ner_dict, measure_exceptions)
        self.non_measure_map = self.generate_non_measure_map()
        if self.debug:
            pprint(self.measure_map)
            pprint(self.non_measure_map)

    def generate_non_measure_map(self):
        tmp = {k: v for k, v in self.ner_dict.items() if k not in self.measure_map.keys()}
        non_measure = dict()
        for k,v in self.ner_dict.items():
            if k in tmp:
                non_measure[k] = [val.lower().strip() for val in v]
        return non_measure

    def encode_advertise(self, advertise):
        self.save_encoded_data(self.ner_tag_advertise(advertise))

    def save_encoded_data(self, *data):
        ad_dict = data
        file_name = os.path.join(self.output_folder, "sequence_enriched.jsonl")
        with open(file_name, "a", encoding="utf-8") as js:
            js.write(json.dumps(ad_dict[0]) + "\n")

    def preload_maps(self, folder: str = None):
        if folder:
            sc.message("Schema loaded !")
            tmp_schema = sc.load_json(folder)
            if "NUMBER_OF_PROPERTIES" in tmp_schema.keys():
                tmp_schema.pop("NUMBER_OF_PROPERTIES")
            return tmp_schema
        else:
            raise ValueError("Parsed properties path cannot be None! Run schema pipe to build parsed props!")

    def save_maps(self):
        pass

    def build_regex(self):
        return {"ngram_rgx": re.compile(r"(\w+|(\*\*[ \w]+\*\*))"),  # match words and **{...}**
                "ngram_clear_rgx":re.compile(r"\*")}

    def get_tagged_sequence(self, terms_input: List[str]) -> "[(str, (str, ...))]":
        """
        Tags tokens from terms_input,
        :param terms_input:
        :return:
        """
        model_input = []

        for term in terms_input:
            tmp_tp: List[str] = []

            for tag, regex_rule_list in self.measure_map.items():
                for regex_rule in regex_rule_list:
                    if re.fullmatch(regex_rule, term):
                        tmp_tp.append(tag)

            for tag, specific_term_list in self.non_measure_map.items():
                if term in specific_term_list:
                    tmp_tp.append(tag)

            cleaned_term = term.strip()

            if len(tmp_tp) == 0:
                model_input.append((cleaned_term, ("O",)))
            else:
                model_input.append((cleaned_term, tmp_tp))

        return model_input


    def ner_tag_advertise(self, advertise: Dict[str, Any]):
        """
        Main method for processing dataset.dat file and mechanically label each token.
        By default, it randomizes on conflict naming entities. One can provide a resolution dictionary
        that avoids randomization on pre-defined solutions.
        Debug mode prints the relevant parts and break the code for the first two observations.
        :return: None
        """
        tmp_ad = advertise.copy()
        full_str: str = sc.debug_print(
            self.splitting_marking(text_input=tmp_ad["clean_text"],
                                   ner_map=self.non_measure_map,
                                   measure_map=self.measure_map), self.debug)

        terms_input: List[str] = sc.debug_print(
            [self.reg_rules["ngram_clear_rgx"].sub("", word[0])
             for word in self.reg_rules["ngram_rgx"].findall(full_str)], self.debug)

        model_input: [(str, (str, ...))] = sc.debug_print(self.get_tagged_sequence(terms_input), self.debug)

        # Build conflict dictionary
        clean_inputs: [(str, str)] = []
        conflict_words = dict()
        for word, ne in model_input:
            if len(ne) > 1:
                if word in conflict_words.keys():
                    for tag in ne:
                        conflict_words[word].add(tag)
                else:
                    conflict_words[word] = {*ne}
                clean_inputs.append((word, random.choice(ne)))
            else:
                clean_inputs.append((word, ne[0]))

        tmp_ad["NER"] = clean_inputs
        return tmp_ad

    def generate_measure_map(self, ner_mapper: Dict[str, Set[str]], fields_exceptions: List[str]) -> Dict[str, Set[str]]:
        new_mapper = dict()
        for key, values in ner_mapper.items():
            if self.check_measure_fields(values) and key not in fields_exceptions:
                new_mapper[key] = self.get_measure_regex(values)

        return new_mapper

    def check_measure_fields(self, values: Set[str], tol: float = 0.7) -> bool:
        """
        Check if field is a 'measure field' - i.e. 64 gb, 5500 mah
        :param values: set of values
        :param tol: tolerance of minimum percentage of fit
        :return: True if is a measure field
        """
        counter: int = 0
        if type(values) is list:
            if len(values) == 0:
                sc.message("WARNING: NER MAPPER HAS A FIELD WITH NO VALUES.")
                return False

            for value in values:
                tmp = value.split()
                if len(tmp) == 2 and tmp[0].isnumeric():
                    counter += 1
            return (counter / len(values)) > tol
        else:
            return False

    def get_measure_regex(self, values: Set[str]) -> Set[str]:
        """
        Get measure regex rule from measure set.
        :param values: set of values
        :return: {"(\d+gb)|(\d+ gb)"}
        """
        measures: Set[str] = set()
        vals = set()

        for value in values:
            tmp = value.split()
            if len(tmp) == 2:
                measures.add(tmp[1])
                vals.add(tmp[0])
        rules = set()
        for measure in measures:
            for val in vals:
                rules.add("( {1} {0} )|( {1}+{0} )".format(measure.lower().strip(), val))
        # return {"( \d+ {0} )|( \d+{0} )".format(measure.lower().strip()) for measure in measures}
        return rules

    def splitting_marking(self, text_input: str, ner_map: Dict[str, List[str]], measure_map: Dict[str, Set[str]] = None) -> str:
        new_txt: str = text_input

        for _, values in ner_map.items():
            for value in values:
                if len(value.split()) > 1:
                    new_txt = re.sub(value, "**{0}**".format(value), new_txt)

        if measure_map:
            pure_match = set()
            for _, values in measure_map.items():
                for rgx in values:
                    matches = re.findall(rgx, new_txt)
                    for match in matches:
                        tmp_ls = list(match)

                        for result in tmp_ls:
                            if result != '':
                                pure_match.add(result)

            if '' in pure_match:
                pure_match.remove('')

            for element in pure_match:
                new_txt = re.sub(element, "**{0}**".format(element), new_txt)

        return new_txt

@gin.configurable
class SequenceReducer(Reducer):
    def __init__(self, main_folder: str, output_folder: str):
        super().__init__(main_folder, output_folder)

    def reduce_process(self):
        workers_folder = [os.path.join(self.main_folder, folder) for folder in os.listdir(self.main_folder)]

        sc.message("Processing files")
        print(workers_folder)

        reduced_folder = sc.check_folder(os.path.join(self.output_folder, "sequence"))

        with open(os.path.join(reduced_folder, "dataset.jsonl"), "a", encoding="utf-8") as js:
            for folderzin in workers_folder:
                for line in open(os.path.join(folderzin, "sequence_enriched.jsonl"), "r", encoding="utf-8"):
                    js.write(line)

        sc.message("DONE !")
