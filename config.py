import logging
import sys
import os


# logging configs
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s [%(module)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler.setFormatter(formatter)
logger.addHandler(handler)


if 'PANDLEAU_HOME' in os.environ:
    PANDLEAU_HOME = os.environ['PANDLEAU_HOME'] + '/'
else:
    PANDLEAU_HOME = '/tmp/'

logger.info(f"PANDLEAU_HOME is {PANDLEAU_HOME}")
