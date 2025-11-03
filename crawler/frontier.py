import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()

        # Check for shelve file with .db extension (most common)
        save_file_exists = os.path.exists(self.config.save_file + '.db') or os.path.exists(self.config.save_file)

        if not save_file_exists and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif save_file_exists and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            # Remove all shelve-related files
            for ext in ['.db', '.dat', '.dir', '.bak', '']:
                try:
                    if os.path.exists(self.config.save_file + ext):
                        os.remove(self.config.save_file + ext)
                except:
                    pass
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if len(self.save) == 0:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = 0
        tbd_count = 0
        corrupted_count = 0

        # Iterate over keys to handle corrupted entries gracefully
        for key in list(self.save.keys()):
            try:
                url, completed = self.save[key]
                total_count += 1
                if not completed and is_valid(url):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1
            except (KeyError, ValueError, EOFError) as e:
                # Skip corrupted entries
                corrupted_count += 1
                self.logger.warning(f"Skipping corrupted entry with key {key}: {e}")
                continue

        if corrupted_count > 0:
            self.logger.warning(f"Skipped {corrupted_count} corrupted entries from shelve")

        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()

    def close(self):
        """Close the shelve database to ensure all data is saved."""
        self.save.close()
        self.logger.info("Frontier shelve database closed successfully.")
