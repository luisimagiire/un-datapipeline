
from typing import Dict, Any, Tuple, Callable

import gin

import pipelines.validation as val
from pipelines.text_processors import full_default_process


def clean_raw_advertise(raw_advertise: Dict[str, Any], parser: "pipelines.parser.Parser"):
    tmp_advertise = parser.get_general_schema(raw_advertise)
    if tmp_advertise and val.valid_advertise(tmp_advertise):
        return clean_base_advertise(tmp_advertise)


@gin.configurable(blacklist=['base_advertise'])
def clean_base_advertise(base_advertise: Dict[str, Any], process_pipeline: Tuple[Callable]):
    tmp_advertise = base_advertise.copy()
    if process_pipeline:
        for process in process_pipeline:
            tmp_advertise = process(tmp_advertise)
    return tmp_advertise


@gin.configurable(blacklist=['raw_text'])
def generate_clean_text(raw_text: str, process_pipeline: Tuple[Callable]):
    clean_txt = raw_text
    ad_process = process_pipeline
    if not ad_process:
        ad_process = (full_default_process, )

    for process in ad_process:
        clean_txt = process(clean_txt)

    return clean_txt

@gin.configurable(blacklist=["advertise"])
def create_clean_text_field(advertise, text_process_pipeline=None):
    tmp_ad = advertise.copy()
    field_name = "clean_text"

    txt_input = build_model_input(advertise, invert=False)
    if text_process_pipeline:
        for process in text_process_pipeline:
            txt_input = process(txt_input)

    tmp_ad[field_name] = txt_input
    return tmp_ad


@gin.configurable(blacklist=["advertise"])
def create_clean_text_field_inverted(advertise, text_process_pipeline=None):
    tmp_ad = advertise.copy()
    field_name = "clean_text_invert"

    txt_input = build_model_input(advertise, invert=True)
    if text_process_pipeline:
        for process in text_process_pipeline:
            txt_input = process(txt_input)

    tmp_ad[field_name] = txt_input
    return tmp_ad


@gin.configurable(blacklist=["text"])
def remove_same_info(text):
    unique = set()
    final = []
    for token in text.split():
        if token not in unique:
            final.append(token)
        unique.add(token)

    return " ".join(final).strip()


def build_model_input(advertise, invert=False):

    if "title" in advertise.keys() and advertise["title"]:
        if "detail" in advertise.keys() and advertise["detail"]:
            if invert:
                if len(advertise["detail"]) + len(advertise["title"]) > 200:
                    delta = (len(advertise["detail"]) - len(advertise["title"]))
                    tmp_input = "{0} {1}".format(advertise["detail"][0:delta], advertise["title"])
                    return tmp_input[0:200]
                else:
                    tmp_input = "{0} {1}".format(advertise["detail"], advertise["title"])
                    return tmp_input
            else:
                tmp_input = "{0} {1}".format(advertise["title"], advertise["detail"])
                if len(tmp_input) > 200:
                    return tmp_input[0:200]
                else:
                    return tmp_input
        else:
            return advertise["title"]
    else:
        raise Exception("Not enough fields in advertise!")
