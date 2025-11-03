from configparser import ConfigParser
from argparse import ArgumentParser
import multiprocessing

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from collections import defaultdict
import pickle
import os
import json

class Stats:
    SAVE_FILE = "stats.pkl"
    FINAL_REPORT = "stats_report.json"

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

    def save_final_report(self):
        """Save comprehensive final stats report"""
        # Get top 100 most common tokens
        sorted_tokens = sorted(self.tokens.items(), key=lambda x: x[1], reverse=True)[:100]

        # Calculate subdomain statistics
        subdomain_stats = {}
        for subdomain, pages in self.subdomains.items():
            subdomain_stats[subdomain] = {
                'unique_pages': len(pages),
                'pages': sorted(list(pages))
            }

        report = {
            'summary': {
                'total_unique_pages': len(self.pages),
                'total_subdomains': len(self.subdomains),
                'longest_page_words': self.longest_length,
                'total_unique_tokens': len(self.tokens),
                'total_token_occurrences': sum(self.tokens.values())
            },
            'subdomains': subdomain_stats,
            'top_100_tokens': [{'token': token, 'count': count} for token, count in sorted_tokens],
            'all_unique_pages': sorted(list(self.pages))
        }

        with open(self.FINAL_REPORT, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n{'='*60}")
        print(f"CRAWL COMPLETE - Final Statistics")
        print(f"{'='*60}")
        print(f"Total Unique Pages Crawled: {len(self.pages)}")
        print(f"Total Unique Subdomains: {len(self.subdomains)}")
        print(f"Longest Page (words): {self.longest_length}")
        print(f"Total Unique Tokens: {len(self.tokens)}")
        print(f"\nTop 10 Most Common Tokens:")
        for i, (token, count) in enumerate(sorted_tokens[:10], 1):
            print(f"  {i}. {token}: {count}")
        print(f"\nSubdomains with Page Counts:")
        for subdomain in sorted(self.subdomains.keys()):
            print(f"  {subdomain}: {len(self.subdomains[subdomain])} pages")
        print(f"\nFull report saved to: {self.FINAL_REPORT}")
        print(f"{'='*60}\n")

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
    # Set multiprocessing start method before any other multiprocessing code
    # Use 'fork' on macOS to avoid pickling issues with the spacetime library
    try:
        multiprocessing.set_start_method('fork', force=True)
    except RuntimeError:
        # Already set, ignore
        pass

    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
