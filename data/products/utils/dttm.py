import datetime
from typing import Optional

from phidata.utils.log import logger

ds_format = '%Y-%m-%d'

def ds_str_to_dttm(
    ds_str: str, ds_format: str = ds_format
) -> Optional[datetime.datetime]:
    """Convert a datestamp string to a Date object"""

    if ds_str and ds_str != "":
        try:
            datetime_object = datetime.datetime.strptime(ds_str, ds_format)
            return datetime_object
        except Exception as e:
            logger.error(e)
            pass
    return None
