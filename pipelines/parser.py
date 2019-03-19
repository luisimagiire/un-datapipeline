import datetime
import hashlib
import logging
import re
from typing import List, Dict, Union, Any, Optional

import gin

from pipelines import utils as sc
from pipelines.clients import CategoryModel
import pipelines.cleaner as dc


@gin.configurable
class Parser:
    def __init__(self, category_model_path: str = None, unique_ids: bool = True, debug: bool = False):
        self.category_model = self.load_category_model(category_model_path)
        self.price_regx = re.compile("\d+(\.\d+|\,\d+)*")
        self.debug = debug
        self.seen_ids = None
        if unique_ids:
            self.seen_ids = set()

    def load_category_model(self, folder_path: str):
        if folder_path:
            return CategoryModel(folder_path)

    def infer_category(self, text: str):
        if not self.category_model:
            raise Exception("Category model was not loaded!")
        return self.category_model.get_category(text)

    def price_parser(self, price_val: Optional[str]):
        if price_val:
            trim_price = price_val.replace(" ", "").replace(".", "").replace(",", ".")
            return float(self.price_regx.search(trim_price).group())

    @staticmethod
    def date_parser(market, datestr):
        """
        Parses datetime scrap field to solr datetime
        :param market: (string) website name (i.e 'OLX')
        :param datestr: (string) datetime string field
        :return: (string) datetime UTC format
        """
        brmonths = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7,
                    'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12}
        year = datetime.datetime.utcnow().year
        solrdateformat = '%Y-%m-%dT%H:%M:%SZ'

        try:
            if market is "olx":
                # target format: "Inserido em: 15 Março às 09:41"
                daymonth = re.search(r'([0-9]+).(' + str.join('|', brmonths.keys()) + ')', datestr)
                hourmin = re.search(r"([0-9]+):([0-9]+)", datestr)
                date = datetime.datetime(year, brmonths[daymonth.group(2)], int(daymonth.group(1)),
                                         hour=int(hourmin.group(1)), minute=int(hourmin.group(2)))
                return date.strftime(solrdateformat)
            else:
                raise ValueError("Market parser not found! Please set a date parser for maket {}.".format(market))
        except ValueError as err:
            logging.debug("Could not parse date for {0} .Using UTC NOW... Error:{1}".format(market, str(err)))
            return datetime.datetime.utcnow().strftime(solrdateformat)

    @staticmethod
    def remove_noise_terms(terms: List[str], noise_terms: List[str]) -> List[str]:
        """
        Remove noise terms from terms list.
        :param terms:
        :param noise_terms:
        :return:
        """
        if '' in terms:
            terms.remove('')

        cp_terms = terms.copy()
        for term in cp_terms:
            for noise in noise_terms:
                if noise in term.lower():
                    terms.remove(term)
                    break

        return terms

    @staticmethod
    def parse_post_category(advertise: Dict[str, Any]) -> Optional[List[str]]:
        """
        Parse 'post_category' field from ML advertise.
        :param advertise: advertise dict from ML
        :return: ['Celulares e Telefones', 'Celulares e Smartphones', 'iPhone', 'iPhone 8 Plus', '64GB']
        """

        noise_terms: List[str] = ["voltar"]

        if "post_category" in advertise.keys():
            tmp: List[str] = re.split('[ \t]{2,}', advertise["post_category"])
            tmp = Parser.remove_noise_terms(tmp, noise_terms)
            return tmp

    @staticmethod
    def parse_user_medals(advertise: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse user medals field from ML advertise.
        :param advertise: ML advertise dict
        :return: None or {'reviews': 153, 'rank': 'mercadolider', 'customer_support': 1, 'good_delivery': 1,
        'sell_freq': 121.75}
        """
        noise_terms: List[str] = ['ver mais dados', 'informação', 'vermelho', 'laranja', 'amarelo', 'verde']

        if "user_medals" in advertise:
            tmp_dict: Dict[str, Any] = {"reviews": None, "rank": None, "customer_support": None, "good_delivery": None}
            tmp: List[str] = re.split('[ \t]{2,}', advertise["user_medals"])

            if str(tmp[0].strip()).isdigit() and "opini" in tmp[1]:
                tmp_dict["reviews"]: int = int(tmp[0].strip())

            tmp = Parser.remove_noise_terms(tmp, noise_terms)

            for term in tmp:

                if "vendas" in term.lower():  # Seller frequency per month
                    seller_freq_term = term
                    seller_freq_ls = re.split("\D+", seller_freq_term)
                    if '' in seller_freq_ls:
                        seller_freq_ls.remove('')
                    if len(seller_freq_ls) == 2 and seller_freq_ls[0].isdigit() and seller_freq_ls[1].isdigit():
                        if "anos" in seller_freq_term:
                            tmp_dict["sell_freq"] = int(seller_freq_ls[0]) / (int(seller_freq_ls[1]) * 12)
                        else:
                            tmp_dict["sell_freq"] = int(seller_freq_ls[0]) / int(seller_freq_ls[1])

                if "MercadoLíder" in term:  # TODO: PROCESS BATCH OF FILES TO GET ALL CASES
                    tmp_dict["rank"] = "mercadolider"

                if "presta um bom atendimento" in term.lower():
                    tmp_dict["customer_support"] = 1

                if "entrega os produtos dentro" in term.lower():
                    tmp_dict["good_delivery"] = 1

            return tmp_dict

    @staticmethod
    def parse_product_full_attributes(advertise: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse 'product_full_attributes' from ML advertise.
        :param advertise: ML advertise dict
        :return: None or {"Marca": "LG", "Tipo de tela": "Plasma", "Tamanho da tela": "50 in"}
        """
        if "product_full_attributes" in advertise.keys():

            tmp_dict: Dict[str, Any] = dict()
            tmp: List[str] = re.split('[ \t]{3,}', advertise["product_full_attributes"])

            for term in tmp:
                key_val: List[str] = re.split('[ \t]{2,}', term)
                if '' in key_val:
                    key_val.remove('')
                if len(key_val) == 2:
                    tmp_dict[re.sub(r'[^\w\s]', '', key_val[0]).strip()] = key_val[1]

            return tmp_dict

    @staticmethod
    def parse_comments_html(advertise: Dict[str, Any]) -> Optional[List[str]]:
        """
        Parse 'comments_html' field from ML advertise.
        :param advertise: ML advertise dict
        :return: List<str>
        """
        if "comments_html" in advertise.keys():

            filtred_comments: str = advertise["comments_html"][200::]

            tmp: List[str] = re.split("[ \n\t]{2,}", filtred_comments)
            if '' in tmp:
                tmp.remove('')

            # Breaking comments
            master: List[List[str]] = []
            tmp_vec: List[str] = []
            for line in tmp:

                if re.search("de \d{4,}", line):  # matches 'de 2018' that signals the end of comment
                    master.append(tmp_vec)
                    tmp_vec = []
                else:
                    tmp_vec.append(line)

            # Cleaning comments
            for comment in master:
                if "..." in comment:
                    comment.remove("...")
                if "O usuário contratou o serviço em" in comment:
                    comment.remove("O usuário contratou o serviço em")

            return [" ".join(m) for m in master]

    @staticmethod
    def parse_product_installment(advertise: Dict[str, Any]) -> Optional[Dict[str, Union[float, int]]]:
        """
        Parses product installment field from ML ads.
        :param advertise: dict(ad)
        :return: {"price":333.2, "parcell": 12, "no_interest": 1}
        """
        if "product_installment" in advertise.keys():

            tmp_dict = {"parcell": None, "price": None, "no_interest": None}
            tmp: List[str] = re.split("[ \t]{2,}", advertise["product_installment"])

            for term in tmp:
                if "x" in term:
                    tmp_vec = term.split()
                    if tmp_vec[0].isnumeric():
                        tmp_dict["parcell"] = int(tmp_vec[0])
                    continue
                if "R$" in term:
                    tmp_vec = term.split()
                    if "R$" in tmp_vec:
                        tmp_vec.remove("R$")
                    if all([term.isnumeric() for term in tmp_vec]):
                        tmp_dict["price"] = float(".".join(tmp_vec))
                    continue
                if "sem juros" in term:
                    tmp_dict["no_interest"] = 1
                    continue
            return tmp_dict

    @staticmethod
    def parse_questions_text(advertise: Dict[str, Any]) -> Optional[List[str]]:
        """
        Parses 'questions_text' field from ML advertise.
        :param advertise: ML advertise dict.
        :return: List of string from questions/answers
        """
        if "questions_text" in advertise.keys():

            tmp: List[str] = advertise["questions_text"].split("Denunciar")
            excepts: List[str] = []

            for part in tmp:

                tmp_vec: List[str] = re.split("[ \t\n]{2,}", part)
                if '' in tmp_vec:
                    tmp_vec.remove('')

                excepts.extend(tmp_vec)

            excepts = set(excepts)

            if '' in excepts:
                excepts.remove('')

            return list(excepts)

    @staticmethod
    def parse_ml_advertise(advertise: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses relevant fields from ML advertise.
        :param advertise: ML advertise dict.
        :return: (parsed) ML advertise dict.
        """
        parsers = [(Parser.parse_post_category, "post_category"),
                   (Parser.parse_user_medals, "user_medals"),
                   (Parser.parse_product_full_attributes, "product_full_attributes"),
                   (Parser.parse_comments_html, "comments_html"),
                   (Parser.parse_product_installment, "product_installment"),
                   (Parser.parse_questions_text, "questions_text")]

        tmp_ad: Dict[str, Any] = advertise.copy()
        for parser, field in parsers:
            if field in tmp_ad.keys():
                tmp_ad[field] = parser(tmp_ad)

        return tmp_ad

    @staticmethod
    def limit_field_size(field_value: str, char_limit: int = 500):
        if field_value:
            if len(field_value) > char_limit:
                return field_value[0:char_limit]
            else:
                return field_value

    def get_detail_field(self, advertise):
        return self.get_field_from_ad(advertise, "detail", ["detail", "post_description"])

    def get_price_field(self, advertise):
        return self.get_field_from_ad(advertise, "price", ["product_price", "price"])

    def get_title_field(self, advertise):
        return self.get_field_from_ad(advertise, "title", ["post_title","title"])

    def get_region_field(self, advertise):
        return self.get_field_from_ad(advertise, "region", ["geo_city", "city"], default_value="brasil")

    def get_date_field(self, advertise):
        return self.get_field_from_ad(advertise, "datetime", ["post_dt_publish", "dt_publish"])

    def get_field_from_ad(self, advertise: Dict[str, Any], field_name: str, alias: List[str], default_value: Optional[str] = None):
        for key in advertise.keys():
            if any([key in field for field in alias]):
                return advertise[key]
        if self.debug:
            sc.message("No {0} column found @ advertise {1}".format(field_name, advertise))
        return default_value

    def get_general_schema(self, adv_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Formats scrapped item dict into SolrSchema basic - without category and tags.
        :param adv_dict: (dict) scrapped item
        :return: (dict/None) solr ready dict
        """

        schema = {}
        advertise = adv_dict.copy()
        schema["url"] = adv_dict["url"]
        hash_object = hashlib.sha1(schema['url'].encode()).hexdigest()
        schema['id'] = hash_object

        # Early stop for non-unique ids
        if self.seen_ids:
            if schema['id'] in self.seen_ids:
                return None
            else:
                self.seen_ids.add(schema['id'])

        if 'olx' in schema['url']:
            schema['market'] = 'OLX'
        elif 'mercadolivre' in schema['url']:
            schema['market'] = 'MercadoLivre'
            advertise = Parser.parse_ml_advertise(adv_dict)
            ml_fields = ["post_category", "user_medals", "product_full_attributes", "comments_html", "product_installment", "questions_text"]
            for field in ml_fields:
                if field in advertise.keys():
                    schema[field] = advertise[field]
        elif 'enjoei' in schema['url']:
            schema['market'] = 'Enjoei'
        else:
            schema['market'] = None

        schema['region'] = self.get_region_field(advertise)
        schema['title'] = self.get_title_field(advertise)
        schema['detail'] = self.limit_field_size(self.get_detail_field(advertise))
        schema['price'] = self.price_parser(self.get_price_field(advertise))
        schema['dt_publish'] = self.get_date_field(advertise)
        schema['datetime'] = self.date_parser(schema['market'], schema["dt_publish"])
        schema['category'] = self.infer_category(dc.build_model_input(schema))

        return schema
