from configparser import ConfigParser
from argparse import ArgumentParser
import multiprocessing

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler

# Fix for macOS pickling error with multiprocessing
multiprocessing.set_start_method('fork', force=True)

from collections import defaultdict

class Stats:
    def __init__(self):
        self.pages = set()
        self.longest_length = 0
        self.tokens = defaultdict(int)
        self.subdomains = defaultdict(int)

    def __repr__(self):
        return f'<Stats:\n pages {self.pages}\n longest_length {self.longest_length}\n tokens {self.tokens}\n subdomains {self.subdomains}\n>'

def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    stats = Stats()
    crawler = Crawler(config, restart, stats)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
