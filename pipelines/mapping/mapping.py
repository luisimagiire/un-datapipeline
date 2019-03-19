import json
import os
from datetime import datetime
from typing import List, Dict

import gin

from pipelines import utils as sc
from pipelines.encoder import BaseEncoder


@gin.configurable
class MappingEncoder(BaseEncoder):
    def __init__(self, seq_max_len: int = 50, max_len_char: int = 10, model_folder: str = None):
        super().__init__()
        self.seq_max_len = seq_max_len
        self.max_len_char = max_len_char
        self.model_folder = sc.check_folder(os.path.join(model_folder, str(datetime.date(datetime.utcnow()))))
        self.model_folder = sc.check_folder(os.path.join(self.model_folder, self.id))
        self.preload_maps()
        self.advertise_counter = 0

    def encode_advertise(self, advertise):
        char2idx = self.maps["char2idx"]
        word2idx = self.maps["word2idx"]

        terms = advertise["clean_text"]
        tmp_seq = self.pad_term_sequence(self.tokenize_sentence(terms), max_len=self.seq_max_len)

        word2idx = self.build_word_representations(tmp_seq, word2idx)
        char2idx = self.build_char_representations(tmp_seq, char2idx)

        # Update maps
        self.maps["char2idx"] = char2idx
        self.maps["word2idx"] = word2idx
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
            self.maps = {"char2idx": {"__PAD__": 0, "UNK": 1}, "word2idx": {"__PAD__": 0, "UNK": 1}}
        else:
            self.maps = {"char2idx": sc.load_pickle(os.path.join(folder, "char_dict.pckl")),
                         "word2idx": sc.load_pickle(os.path.join(folder, "word_dict.pckl"))}

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
