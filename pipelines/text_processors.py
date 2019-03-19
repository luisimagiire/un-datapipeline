import string
import unicodedata
import gin
import re


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def clean_text(text):
    """
    Strip accents and lower text string
    :param text: (str) text to be cleaned
    :return: (str) cleaned text
    """
    text = strip_accents(text)
    text = text.lower().strip()
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text


def get_corpus_input(data, fields=('title', 'detail')):
    """
    Given data dict, concatenate chosen fields.
    :param data: (dict) data point to be scrapped
    :param fields: (tp) fields to be used
    :return: (str) concatenated corpus
    """
    base_string = ""
    for field in fields:
        if field not in data.keys():
            raise Exception("Field {} is not included in data keys!".format(field))
        if type(data[field]) is list and len(data[field]) > 0:
            tmp_str = " ".join(data[field])
            base_string += tmp_str + " "
            continue
        if field == 'category':
            base_string += normalize_compose_terms(data[field]) + " "
            continue
        base_string += data[field] + " "

    return base_string[:-1]


def get_corpus_output(data, fields=('category', 'tags')):
    return get_corpus_input(data, fields=fields)


def tokenize(data, sep=None):
    if sep is not None:
        return data.split(sep)
    return data.split()

@gin.configurable
def full_default_process(data):
    sw = StopWords()
    return " ".join(
        sw.filter_tokens(
            tokenize(
                remove_numbers(
                    clean_text(data)))))


def remove_numbers(text):
    return re.sub(r'\b[0-9]+\b', '', text)


def normalize_compose_terms(term):
    if len(term.split()) > 1:
        return "-".join(term.split()).lower()
    return term.lower()


def process_targets(data):
    """
    Removes region and market keywords, plus normalizing the remaining terms.
    :param data: (string) target
    :return: (str) clean target string
    """
    def drop_filter(token):
        return "S_" in token or "R_" in token
    tmp_list = [tag for tag in tokenize(strip_accents(data)) if not drop_filter(tag)]
    return " ".join(tmp_list)


class StopWords:
    words = {'minhas', 'aquelas', 'estiveram', 'formos', 'tinham', 'já', 'nossa', 'será', 'não', 'muito', 'aqueles',
             'numa', 'esteve', 'estejamos', 'estivessem', 'houvessem', 'houveremos', 'há', 'esse', 'estamos', 'terei',
             'ou', 'hao', 'seriam', 'estivermos', 'fosse', 'teremos', 'houverão', 'das', 'só', 'houveram', 'nossos',
             'houveriam', 'a', 'para', 'fui', 'sera', 'for', 'tém', 'tera', 'tiveram', 'sejam', 'vocês', 'teriam', 'ha',
             'houveramos', 'houve', 'teria', 'estivemos', 'estivéssemos', 'serei', 'tivesse', 'houvessemos', 'essas',
             'houverei', 'houveria', 'da', 'tivessem', 'terá', 'fôssemos', 'teriamos', 'pela', 'quando', 'teu', 'você',
             'fora', 'estiver', 'esta', 'era', 'e', 'seremos', 'tiver', 'serao', 'éramos', 'seus', 'seríamos',
             'foramos', 'eramos', 'forem', 'meu', 'o', 'houvéramos', 'houveriamos', 'tínhamos', 'este', 'lhes', 'sou',
             'esteja', 'estão', 'aquele', 'entre', 'houvéssemos', 'foram', 'mesmo', 'estivesse', 'hajam', 'teríamos',
             'também', 'nas', 'um', 'estejam', 'tu', 'minha', 'houver', 'seja', 'no', 'dos', 'deles', 'voce', 'está',
             'aquela', 'hão', 'aquilo', 'mais', 'haja', 'estes', 'terão', 'estao', 'tiveramos', 'isso', 'estivera',
             'terao', 'houvemos', 'se', 'sejamos', 'tiverem', 'esses', 'são', 'os', 'lhe', 'nosso', 'uma', 'dela',
             'tambem', 'eles', 'estavamos', 'fossemos', 'como', 'houvermos', 'pelos', 'tenham', 'houverem', 'so',
             'fomos', 'estava', 'tivemos', 'tivéramos', 'seu', 'essa', 'às', 'te', 'nao', 'nos', 'pelo', 'sua', 'foi',
             'ao', 'tivermos', 'hei', 'seriamos', 'quem', 'tive', 'tenha', 'tivessemos', 'tinhamos', 'houvesse',
             'estive', 'num', 'qual', 'em', 'que', 'fossem', 'sem', 'com', 'mas', 'aos', 'houveríamos', 'sao',
             'estavam', 'voces', 'teve', 'seria', 'estivéramos', 'tivera', 'de', 'ele', 'elas', 'ja', 'tivéssemos',
             'ate', 'estivessemos', 'tem', 'serão', 'do', 'ela', 'delas', 'suas', 'dele', 'pelas', 'houverá', 'houvera',
             'houverao', 'estávamos', 'nem', 'fôramos', 'nós', 'estiveramos', 'me', 'por', 'tenhamos', 'na', 'havemos',
             'eram', 'estas', 'as', 'à', 'tua', 'somos', 'hajamos', 'tuas', 'tinha', 'estiverem', 'teus', 'tenho',
             'meus', 'nossas', 'estou', 'temos', 'até', 'depois', 'vos', 'eu', 'isto', 'eh'}

    def filter_tokens(self, text_tokens):
        """
        Given text_tokens, filter all tokens that are stopwords.
        :param text_tokens: (list<str>) tokens to be filtered
        :return: (list<str>) clean tokens
        """
        clean_tokens = [token.strip() for token in text_tokens if token not in self.words and len(token) > 1]
        return clean_tokens
