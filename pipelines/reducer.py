import abc
import pipelines.utils as sc
import os
from datetime import datetime


class Reducer(object, metaclass=abc.ABCMeta):
    today_date = str(datetime.date(datetime.utcnow()))

    def __init__(self, main_folder: str, output_folder: str, debug: bool=False):
        self.main_folder = sc.check_folder(os.path.join(main_folder, self.today_date))
        self.output_folder = sc.check_folder(os.path.join(output_folder, self.today_date))
        self.debug = debug

    @abc.abstractmethod
    def reduce_process(self):
        raise NotImplementedError('User must define specific reduce method !')
