from loguru import logger
from config import env_vars
import sys

logger.remove(0)
logger.add(sys.stdout, level="DEBUG")
