import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from utils.tokenize import tokenize_text
import json
from datetime import datetime


# Skip blocked domains and domain+path combinations
blocked_domains = {"transformativeplay.ics.uci.edu", "tron.lom.ics.uci.edu",
                   "san-tainer-1.lom.ics.uci.edu",
                   "mt-live.ics.uci.edu", "fano.ics.uci.edu", "sli.ics.uci.edu",
                   "sprout.ics.uci.edu",
                   "checkmate.ics.uci.edu", "tippersweb.ics.uci.edu"}
blocked_paths = {"ics.uci.edu/people/", "www.ics.uci.edu/~eppstein/gina/", "www.ics.uci.edu/~wjohnson/"}

def save_stats_log(stats, url):
    log_entry = {
        'pages_scraped': len(stats.pages),
        # 'pages': list(stats.pages),
        # 'subdomains': {subdomain: list(pages) for subdomain, pages in stats.subdomains.items()},
        # 'tokens': dict(stats.tokens),
        'longest_page_words': stats.longest_length
    }

    with open('logs/Stats.log', 'w') as f:
        json.dump(log_entry, f, indent=2)

def scraper(url, resp, stats, stopwords):
    if resp.status != 200:
        return []

    links = extract_next_links(url, resp)

    soup = BeautifulSoup(resp.raw_response.content, 'lxml')

    # Extract text content from HTML and tokenize (skipping stopwords)
    text_content = soup.get_text(separator = ' ', strip = True)

    # Count total words (including stopwords) for longest page tracking
    words = text_content.split()
    word_count = len([word for word in words if word])
    if word_count > stats.longest_length:
        stats.longest_length = word_count

    # Tokenize and merge token frequencies into global stats (excluding stopwords)
    tokens = tokenize_text(text_content, stopwords)
    for token, count in tokens.items():
        stats.tokens[token] += count

    # Track subdomain for successfully crawled pages
    parsed_url = urlparse(url)
    if parsed_url.netloc.endswith('.uci.edu') or parsed_url.netloc == 'uci.edu':
        subdomain = parsed_url.netloc
        # Remove fragment from URL for unique page tracking
        page_url = urldefrag(url)[0]

        # Add this page to the subdomain's unique pages
        stats.subdomains[subdomain].add(page_url)

    # Save stats to log after processing this page
    save_stats_log(stats, url)

    valid_links = []
    for link in links:
        if is_valid(link, stats):
            valid_links.append(link)
            # Remove fragment and add to stats.pages (set automatically handles uniqueness)
            url_without_fragment = urldefrag(link)[0]
            stats.pages.add(url_without_fragment)
    return valid_links

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = []
    if resp.status != 200:
        return links

    MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB in bytes
    if resp.raw_response and resp.raw_response.content:
        content_size = len(resp.raw_response.content)
        if content_size > MAX_FILE_SIZE:
            print(f"Skipping large file ({content_size / (1024*1024):.2f} MB): {url}")
            return links

    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')

        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(resp.url, href)
            links.append(absolute_url)

    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links

def is_valid(url, stats=None):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    try:
        parsed = urlparse(url)

        # Skip if base URL (without query/fragment) was already crawled
        # Only check duplicates if stats is provided
        if stats:
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if base_url in stats.pages: # maybe too restrictive? allow fragments but not count them as unique pages?
                return False

        if parsed.scheme not in {"http", "https"}:
            return False

        # allowed domains
        allowed_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        domain_valid = False
        for domain in allowed_domains:
            if parsed.netloc == domain or parsed.netloc.endswith("." + domain):
                domain_valid = True
                break

        if not domain_valid:
            return False

        # Block entire domains
        if parsed.netloc in blocked_domains:
            return False

        # Block specific domain+path combinations
        for blocked in blocked_paths:
            if '/' in blocked:
                domain, path = blocked.split('/', 1)
                if parsed.netloc == domain and parsed.path.startswith('/' + path):
                    return False

        # Skip paths with single dash segment (/-/)
        if '/-/' in parsed.path:
            return False
        #if '/activity' in parsed.path: # not necessary?
            #return False
        if '/doku.php/' in parsed.path:
            return False

        # date avoiding
        date_pattern_slash = r'/\d{4}/\d{1,2}(/\d{1,2})?'
        if re.search(date_pattern_slash, parsed.path):
            return False

        date_pattern_hyphen = r'\d{4}-\d{1,2}(-\d{1,2})?'
        if re.search(date_pattern_hyphen, parsed.path):
            return False

        # Patterns: /page/2, /p/3, /news/page/2, etc.
        pagination_pattern = r'/(page|p)/\d+/?'
        if re.search(pagination_pattern, parsed.path.lower()):
            return False

        # Match patterns like: /r123.html, /paper456.php, /item789.aspx
        # maybe allow this?
        numbered_file_pattern = r'/([a-z]+)(\d+)\.(html?|php|aspx?)$'
        match = re.search(numbered_file_pattern, parsed.path.lower())
        if match:
            prefix = match.group(1)  # e.g., "r", "paper", "item"
            number = int(match.group(2))  # e.g., 83, 456

            # Allow up to 50 numbered items per pattern
            if number > 50:
                return False

        # Detect Git repository paths with commit hashes (crawler traps)
        git_pattern = r'/(tree|commit|blob|raw)s?/[a-f0-9]{30,}'
        if re.search(git_pattern, parsed.path.lower()):
            return False

        # Detect any URL with very long hexadecimal strings (likely identifiers/hashes)
        # is this necessary?
        long_hex_pattern = r'/[a-f0-9]{32,}'
        if re.search(long_hex_pattern, parsed.path.lower()):
            return False

        # Skip image gallery paths
        gallery_patterns = ['/pix/', '/photos/', '/gallery/', '/galleries/', '/images/', '/pics/']
        path_lower = parsed.path.lower()
        if any(pattern in path_lower for pattern in gallery_patterns):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise