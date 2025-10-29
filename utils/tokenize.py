from collections import defaultdict

# runtime: O(n) since runs through each character once
def tokenize_text(text: str, stop_words: set = None) -> defaultdict:
    """Tokenize text content directly and count token frequencies.

    Args:
        text: The text to tokenize
        stop_words: Optional set of words to skip during tokenization

    Returns:
        defaultdict(int) with token frequencies (excluding stopwords if provided)
    """
    text = text.lower()
    cur = 0
    index = 0
    token_freq = defaultdict(int)
    while index < len(text):
        ch = text[index]
        if not (('a' <= ch <= 'z') or ('A' <= ch <= 'Z') or ('0' <= ch <= '9')):
            token = text[cur:index]
            # Only count token if it's not empty and not a stopword
            if token and (stop_words is None or token not in stop_words):
                token_freq[token] += 1
            cur = index + 1
        index += 1

    token = text[cur:index]
    # Only count token if it's not empty and not a stopword
    if token and (stop_words is None or token not in stop_words):
        token_freq[token] += 1
    return token_freq