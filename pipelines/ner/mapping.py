import json
import os
from datetime import datetime
from typing import List, Dict

import gin

from pipelines import utils as sc
from pipelines.encoder import BaseEncoder
from pipelines.reducer import Reducer


@gin.configurable
class NERMappingEncoder(BaseEncoder):
    def __init__(self, seq_max_len: int = 50, max_len_char: int = 10, model_folder: str = None, debug: bool = False):
        super().__init__()
        self.seq_max_len = seq_max_len
        self.max_len_char = max_len_char
        self.model_folder = sc.check_folder(os.path.join(model_folder, str(datetime.date(datetime.utcnow()))))
        self.model_folder = sc.check_folder(os.path.join(self.model_folder, self.id))
        self.preload_maps()
        self.advertise_counter = 0
        self.debug = debug

    def encode_advertise(self, advertise):
        char2idx = self.maps["char2idx"]
        word2idx = self.maps["word2idx"]
        tag2idx = self.maps["tag2idx"]

        terms = advertise["NER"]
        terms_words = [token[0] for token in terms]
        terms_tags = [token[1] for token in terms]

        if self.debug:
            print(terms_words)
            print(terms_tags)

        word2idx = self.build_word_representations(terms_words, word2idx)
        char2idx = self.build_char_representations(terms_words, char2idx)
        tag2idx = self.build_tag_representations(terms_tags, tag2idx)


        # Update maps
        self.maps["char2idx"] = char2idx
        self.maps["word2idx"] = word2idx
        self.maps["tag2idx"] = tag2idx
        self.advertise_counter += 1
        sc.get_notice(self.advertise_counter, 5000, msg_text="ads processed!")

    def build_tag_representations(self, terms, tagidx):
        return self.build_word_representations(terms, tagidx)

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
            dataset_name = "dataset.jsonl"
            dataset_path = os.path.join(self.model_folder, dataset_name)

            with open(dataset_path, "a", encoding="utf-8") as js:
                x_word, x_char, y_price = data
                for word, char, price in zip(x_word, x_char, y_price):
                    js.write(json.dumps({"x_word": word, "x_char": char, "y_price": price}) + "\n")
                    self.processed_counter += 1
                    sc.get_notice(self.processed_counter, msg_text="training obs processed!")
        else:
            # TODO: implement saving in DataStorage
            raise NotImplementedError("Saving outside a local folder path is not implemented yet!")

    def preload_maps(self, folder: str= None):
        if not folder:
            self.maps = {"char2idx": {"__PAD__": 0, "UNK": 1},
                         "word2idx": {"__PAD__": 0, "UNK": 1},
                         "tag2idx": {"__PAD__": 0}}
        else:
            self.maps = {"char2idx": sc.load_pickle(os.path.join(folder, "char_dict.pckl")),
                         "word2idx": sc.load_pickle(os.path.join(folder, "word_dict.pckl")),
                         "tag2idx": sc.load_pickle(os.path.join(folder, "target_dict.pckl"))}

    @staticmethod
    def tokenize_sentence(sentence: str):
        return sentence.split()

    def build_char_representations(self, sentence: List[str], charidx: Dict[str, int]):
        charidxer = charidx.copy()

        while '' in sentence:
            sentence.remove('')

        for i in range(self.seq_max_len):
            for j in range(self.max_len_char):
                try:
                    idx = charidxer.get(sentence[i][j])
                    if not idx:
                        charidxer[sentence[i][j]] = len(charidxer.keys()) + 1
                except:
                    pass

        return charidxer

    def build_word_representations(self, sentence: List[str], wordidx: Dict[str, int]):
        wordidxer = wordidx.copy()

        for w in sentence:
            if w not in wordidxer.keys():
                wordidxer[w] = len(wordidxer.keys()) + 1

        return wordidxer

    @staticmethod
    def pad_term_sequence(sequence: List[str], max_len: int) -> List[str]:
        """
        Pad word sequence to max lenght using '__PAD__' if len(sequence) is lower than max_len.
        :param sequence: List of words
        :param max_len: Maximum lenght of padded sequence
        :return: Padded sequence
        """
        padded_sequence: List[str] = []
        for indx in range(max_len):
            try:
                padded_sequence.append(sequence[indx])
            except:
                padded_sequence.append("__PAD__")

        return padded_sequence


@gin.configurable
class NERMappingReducer(Reducer):

    def __init__(self, main_folder: str, output_folder: str, debug: bool = False):
        super().__init__(main_folder, output_folder, debug=debug)

    def reduce_process(self):
        workers_folder = [os.path.join(self.main_folder, folder) for folder in os.listdir(self.main_folder)]

        if self.debug:
            print("Paths being aggregated...")
            print(workers_folder)

        char2idx = set()
        word2idx = set()
        tag2idx = set()

        sc.message("Processing files...")

        for folderzin in workers_folder:
            tmp_chars = sc.load_json(os.path.join(folderzin, "char2idx.json"))
            tmp_words = sc.load_json(os.path.join(folderzin, "word2idx.json"))
            tmp_tags = sc.load_json(os.path.join(folderzin, "tag2idx.json"))

            for c in tmp_chars.keys():
                char2idx.add(c)

            for w in tmp_words.keys():
                word2idx.add(w)

            for t in tmp_tags.keys():
                tag2idx.add(t)

        reduced_folder = sc.check_folder(os.path.join(self.output_folder, "ner_mapping"))
        basec2i = {"__PAD__": 0, "UNK": 1}
        basew2i = {"__PAD__": 0, "UNK": 1}
        baset2i = {"__PAD__": 0}

        char2idx.remove("__PAD__")
        char2idx.remove("UNK")
        word2idx.remove("__PAD__")
        word2idx.remove("UNK")
        tag2idx.remove("__PAD__")
        if "UNK" in tag2idx:
            tag2idx.remove("UNK")

        for i, c in enumerate(char2idx):
            basec2i[c] = i + 2

        for i, c in enumerate(word2idx):
            basew2i[c] = i + 2

        for i, c in enumerate(tag2idx):
            baset2i[c] = i + 1

        sc.message("Saving chars")
        sc.save_dict_2json(os.path.join(reduced_folder, "char2idx.json"), basec2i)
        sc.message("Saving words")
        sc.save_dict_2json(os.path.join(reduced_folder, "word2idx.json"), basew2i)
        sc.message("Saving tags")
        sc.save_dict_2json(os.path.join(reduced_folder, "tag2idx.json"), baset2i)
