import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from utils.tokenize import tokenize_text
import json
from datetime import datetime

def save_stats_log(stats, url):
    log_entry = {
        'pages': list(stats.pages),
        'subdomains': {subdomain: list(pages) for subdomain, pages in stats.subdomains.items()},
        'tokens': dict(stats.tokens),
        'longest_page_words': stats.longest_length
    }

    with open('logs/Stats.log', 'w') as f:
        json.dump(log_entry, f, indent=2)

def scraper(url, resp, stats, stopwords):
    if resp.status != 200:
        return []

    links = extract_next_links(url, resp)

    soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    print(soup.prettify())

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
        if is_valid(link):
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

def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    # check if a right button goes on forever
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
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

        # no queries
        if parsed.query:
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