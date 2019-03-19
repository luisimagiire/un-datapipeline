from typing import Dict, Any, Tuple

import gin

from pipelines import utils as sc


@gin.configurable
def valid_advertise(advertise: Dict[str, Any],
                    minimum_cols: Tuple[str] = ('price', 'url'),
                    category: str = None,
                    market: str = None,
                    silent:bool= True) -> bool:
    """
    Checks if there are any missing crucial fields.
    In case of missing, adds default value (if there is one) or raises error.
    :param adv_dict: (dict) scrapped item
    :return: (bool) valid or not
    """

    try:
        # Checks if data has all necessary columns
        for col in minimum_cols:
            if col not in advertise.keys() or not advertise[col]:
                raise Exception('{0}:{1} - Missing necessary column: {2}'.format(
                    advertise["id"], "Missing Value", str(col)))
            elif col == "price" and not validate_prices(advertise[col], advertise["category"]):
                raise Exception('{0}:{1} - Invalid price for category: {2} of {3}'.format(
                    advertise["id"], "Invalid Price", advertise["category"], advertise["price"]))
            elif category is not None and advertise["category"] != category:
                raise Exception('{0}:{1} - Invalid category: {2}'.format(
                    advertise["id"], "Invalid Category", advertise["category"]))
            elif market is not None and advertise["market"] != market:
                raise Exception('{0}:{1} - Invalid market: {2}'.format(
                    advertise["id"], "Invalid Market", advertise["market"]))

        return True
    except Exception as err:
        if not silent:
            sc.message(str(err))
        return False


def validate_prices(price: float, category: str):
    high_price_cats = ["veiculos", "imoveis"]
    if category in high_price_cats:
        return price > 10000
    return price > 10 and price < 10000
