from datetime import datetime

from pipelines.encoder import BaseEncoder
import numpy as np
from typing import List, Dict, Union, Any, Optional, Tuple
import gin
import os
from pipelines import utils as sc
import json
from keras.preprocessing.sequence import pad_sequences


@gin.configurable
class PricingEncoder(BaseEncoder):
    def __init__(self, seq_max_len: int = 50, max_len_char: int = 10, model_folder: str = None, maps_folder: str = None, update_maps: bool = False):
        super().__init__()
        self.seq_max_len = seq_max_len
        self.max_len_char = max_len_char
        self.model_folder = sc.check_folder(os.path.join(model_folder, str(datetime.date(datetime.utcnow()))))
        self.model_folder = sc.check_folder(os.path.join(self.model_folder, self.id))
        self.preload_maps(maps_folder)
        self.advertise_counter = 0
        self.processed_counter = 0
        self.update_maps = update_maps

    def encode_advertise(self, advertise):
        x_char, x_word, y_price = [], [], []
        char2idx = self.maps["char2idx"]
        word2idx = self.maps["word2idx"]

        terms = advertise["clean_text"]
        tmp_seq = self.pad_term_sequence(self.tokenize_sentence(terms), max_len=self.seq_max_len)

        w_rep, word2idx = self.build_word_representations(tmp_seq, word2idx)
        x_word.append(pad_sequences(maxlen=self.seq_max_len, sequences=[w_rep], value=word2idx["__PAD__"],
                                    padding='post', truncating='post').tolist())
        representation, char2idx = self.build_char_representations(tmp_seq, char2idx)
        x_char.append(representation)
        y_price.append(np.log(float(advertise["price"])))

        # Translate position of title and detail
        terms = advertise["clean_text_invert"]
        tmp_seq = self.pad_term_sequence(self.tokenize_sentence(terms), max_len=self.seq_max_len)

        w_rep, word2idx = self.build_word_representations(tmp_seq, word2idx)
        x_word.append(pad_sequences(maxlen=self.seq_max_len, sequences=[w_rep], value=word2idx["__PAD__"],
                                    padding='post', truncating='post').tolist())
        representation, char2idx = self.build_char_representations(tmp_seq, char2idx)
        x_char.append(representation)
        y_price.append(np.log(float(advertise["price"])))

        self.save_encoded_data(x_word, x_char, y_price)

        # Update maps
        if self.update_maps:
            self.maps["char2idx"] = char2idx
            self.maps["word2idx"] = word2idx

        self.advertise_counter += 1
        sc.get_notice(self.advertise_counter, 5000, msg_text="ads processed!")

    def save_maps(self, *maps):
        sc.message("Saving Maps...")
        if self.model_folder:
            for k, map in self.maps.items():
                # TODO: may get too big, write as txt
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
            self.maps = {"char2idx": sc.load_json(os.path.join(folder, "char2idx.json")),
                         "word2idx": sc.load_json(os.path.join(folder, "word2idx.json"))}

    @staticmethod
    def tokenize_sentence(sentence: str):
        return sentence.split()

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

    def build_char_representations(self, sentence: List[str], charidx: Dict[str, int]):
        charidxer = charidx.copy()

        while '' in sentence:
            sentence.remove('')

        sent_seq = []
        for i in range(self.seq_max_len):
            word_seq = []
            for j in range(self.max_len_char):
                try:
                    idx = charidxer.get(sentence[i][j])
                    if idx:
                        word_seq.append(idx)
                    else:
                        if self.update_maps:
                            charidxer[sentence[i][j]] = len(charidxer.keys()) + 1
                            word_seq.append(charidxer.get(sentence[i][j]))
                        else:
                            word_seq.append(charidxer.get("UNK"))
                except:
                    word_seq.append(charidxer.get("__PAD__"))
            sent_seq.append(word_seq)

        return sent_seq, charidxer

    def build_word_representations(self, sentence: List[str], wordidx: Dict[str, int]):
        wordidxer = wordidx.copy()
        sentence_w = []

        for w in sentence:
            if w in wordidxer.keys():
                sentence_w.append(wordidxer[w])
            else:
                if self.update_maps:
                    wordidxer[w] = len(wordidxer.keys()) + 1
                    sentence_w.append(wordidxer[w])
                else:
                    sentence_w.append(wordidxer["UNK"])

        return sentence_w, wordidxer
