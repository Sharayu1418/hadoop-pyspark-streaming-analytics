#!/usr/bin/env python3
"""
Mapper for Word Count with Statistics
Reads text from stdin, processes each word, and emits key-value pairs
"""
import sys
import re
import string

# Define stop words to filter out
STOP_WORDS = {'the', 'is', 'an', 'a', 'are'}

def process_line(line):
    """
    Process a line of text and emit key-value pairs
    Emits: word_count:<word> 1 and word_length:<length> 1
    """
    # Convert to lowercase
    line = line.lower()
    
    # Remove punctuation and split into words
    # Keep only alphabetic characters
    words = re.findall(r'[a-z]+', line)
    
    for word in words:
        # Filter out stop words
        if word not in STOP_WORDS and len(word) > 0:
            # Emit word count: key format "word_count:word\t1"
            print(f"word_count:{word}\t1")
            
            # Emit word length: key format "word_length:length\t1"
            word_len = len(word)
            print(f"word_length:{word_len}\t1")
            
            # Emit total word count marker
            print(f"total_words:all\t1")

def main():
    """
    Main mapper function - reads from stdin
    """
    try:
        for line in sys.stdin:
            line = line.strip()
            if line:  # Skip empty lines
                process_line(line)
    except Exception as e:
        sys.stderr.write(f"Mapper Error: {str(e)}\n")

if __name__ == "__main__":
    main()
