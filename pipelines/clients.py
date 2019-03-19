import json
import os
import pickle

import gin
import numpy as np
from keras.models import model_from_json
from keras.preprocessing.sequence import pad_sequences

from pipelines import utils as sc
from pipelines.text_processors import strip_accents


@gin.configurable
class CategoryModel:
    def __init__(self, model_folder: str, cutoff: float = 0.9):
        self.model = self.load_model(model_folder)
        self.encoders = self.load_encoders(model_folder)
        self.category_cutoff=cutoff

    def load_model(self, model_path: str):
        model_json = os.path.join(model_path, "model_arc.json")
        model_weights = [os.path.join(model_path, x) for x in os.listdir(model_path) if 'weights' in x]
        with open(model_json, 'r') as dt:
            model_arc = json.load(dt)
        model = model_from_json(json.dumps(model_arc))
        model.load_weights(model_weights[0])
        sc.message("Category model loaded!")
        return model

    def load_encoders(self, model_folder: str):
        """
        Load encoders from pickle.
        :return: list of dicts with name and the encoder object
        """
        encoders_path = os.path.join(model_folder, "encoders")

        if not os.path.isdir(encoders_path):
            raise Exception("Encoders folder was not found!")

        encoders = [os.path.join(encoders_path, encoder_file) for encoder_file in os.listdir(encoders_path)]
        unpickled_encoders = []

        for label_path in encoders:
            if label_path.endswith('.pckl'):
                with open(label_path, 'rb') as dt:
                    unpickled_encoders.append({'name': os.path.split(label_path)[-1], 'encoder': pickle.load(dt)})

        if len(unpickled_encoders) != 3:
            raise Exception("There are missing encoders. Please check model/encoders folder!")

        return unpickled_encoders

    def get_category(self, text_input: str):

        # Tokenize text_input
        tokenizer, cat_encoder = None, None
        for label in self.encoders:
            if "category" in label["name"]:
                cat_encoder = label["encoder"]
            elif "tokenizer" in label["name"]:
                tokenizer = label["encoder"]

        padding = self.model.get_layer('text_input').input_shape[1]
        model_input = self.tokenize_input(text_input, tokenizer, padding)
        _, cat_pred = self.model.predict_on_batch(model_input)
        category = self.format_category(cat_pred, cat_encoder)

        return category

    @staticmethod
    def tokenize_input(text_input, tokenizer, padding):
        sequence = tokenizer.texts_to_sequences([text_input])
        sequence = pad_sequences(sequence, maxlen=padding, padding='post').tolist()
        return np.array(sequence)

    def format_category(self, category_prediction, encoder):
        category = []
        sufficient_prediction = [x for x in category_prediction[0] if x > self.category_cutoff]
        if len(sufficient_prediction) == 0:
            category.append("variados")
        else:
            category = encoder.inverse_transform(category_prediction, 0)
        return strip_accents(category[0])
