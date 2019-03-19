import abc

import gin
import secrets

class BaseEncoder(object, metaclass=abc.ABCMeta):

    def __init__(self, debug=False):
        self.maps = None
        self.id: str = str(secrets.token_hex(nbytes=16))
        self.debug = debug

    @abc.abstractmethod
    def encode_advertise(self, advertise):
        raise NotImplementedError('User must define specific encoding method !')

    @gin.configurable
    def batch_encode_stream(self, advertise_generator, batch: int = None):
        if batch:
            try:
                batch_data = []
                for data in advertise_generator:
                    batch_data.append(self.encode_advertise(data))
                    if len(batch_data) % batch:
                        yield batch_data
                        batch_data = []
            except StopIteration:
                return None
        else:
            try:
                for data in advertise_generator:
                    yield self.encode_advertise(data)
            except StopIteration:
                return None

    @abc.abstractmethod
    def save_encoded_data(self):
        raise NotImplementedError('User must define output saving for encodings !')

    @abc.abstractmethod
    def preload_maps(self, folder: str = None):
        raise NotImplementedError('User must define specific maps for models\' encoding !')

    @abc.abstractmethod
    def save_maps(self):
        raise NotImplementedError('User must define save maps for models\' encoding !')
