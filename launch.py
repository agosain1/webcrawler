from configparser import ConfigParser
from argparse import ArgumentParser
import multiprocessing

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler

# Fix for macOS pickling error with multiprocessing
multiprocessing.set_start_method('fork', force=True)

from collections import defaultdict
import pickle
import os

class Stats:
    SAVE_FILE = "stats.pkl"

    def __init__(self):
        self.pages = set()
        self.longest_length = 0
        self.tokens = defaultdict(int)
        self.subdomains = defaultdict(set)

    def __repr__(self):
        return f'<Stats:\n pages {self.pages}\n longest_length {self.longest_length}\n tokens {self.tokens}\n subdomains {self.subdomains}\n>'

    def save(self):
        with open(self.SAVE_FILE, 'wb') as f:
            pickle.dump({
                'pages': self.pages,
                'longest_length': self.longest_length,
                'tokens': dict(self.tokens),
                'subdomains': {k: v for k, v in self.subdomains.items()}
            }, f)

    @staticmethod
    def load():
        if not os.path.exists(Stats.SAVE_FILE):
            return Stats()

        try:
            with open(Stats.SAVE_FILE, 'rb') as f:
                data = pickle.load(f)
                stats = Stats()
                stats.pages = data['pages']
                stats.longest_length = data['longest_length']
                stats.tokens = defaultdict(int, data['tokens'])
                stats.subdomains = defaultdict(set, data['subdomains'])
                return stats
        except Exception as e:
            print(f"Error loading stats: {e}")
            return Stats()

def _get_stop_words() -> set[str]:
    stop_words = set()
    with open('stopwords.txt', 'r') as f:
        for line in f:
            word = line.strip()
            if word:
                stop_words.add(word.lower())
    return stop_words

def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)

    if restart:
        if os.path.exists(Stats.SAVE_FILE):
            os.remove(Stats.SAVE_FILE)
            print(f"Deleted existing stats file: {Stats.SAVE_FILE}")
        stats = Stats()
    else:
        stats = Stats.load()
        print(f"Loaded {len(stats.pages)} pages from previous crawl")

    stopwords = _get_stop_words()
    crawler = Crawler(config, restart, stats, stopwords)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
