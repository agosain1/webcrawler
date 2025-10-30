import signal
from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, stats, stopwords, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        self.stats = stats
        self.stopwords = stopwords
        self.shutdown_flag = False

        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT (Control+C) gracefully"""
        self.logger.info("\n\nReceived shutdown signal. Finishing current tasks and saving...")
        self.shutdown_flag = True

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.stats, self.stopwords, self)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
        # Close the frontier to ensure shelve is saved
        self.frontier.close()
        self.logger.info("Crawler stopped. All data saved.")
