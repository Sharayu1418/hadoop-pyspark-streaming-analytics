#!/usr/bin/env python3
"""
Reducer for Word Count with Statistics
Aggregates mapper output to produce final statistics
"""
import sys
from collections import defaultdict
from operator import itemgetter

def main():
    """
    Main reducer function - reads sorted key-value pairs from stdin
    Aggregates counts for words, word lengths, and totals
    """
    # Data structures for aggregation
    word_counts = defaultdict(int)
    length_counts = defaultdict(int)
    total_words = 0
    current_key = None
    current_count = 0
    try:
        # Process each line from mapper output
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                # Parse key and value
                key, value = line.split('\t', 1)
                count = int(value)
                # Parse the key type and actual key
                if ':' in key:
                    key_type, key_data = key.split(':', 1)
                    if key_type == "word_count":
                        # Aggregate word counts
                        word_counts[key_data] += count
                    elif key_type == "word_length":
                        # Aggregate word length distribution
                        length_key = int(key_data)
                        length_counts[length_key] += count
                    elif key_type == "total_words":
                        # Count total words
                        total_words += count
            except ValueError as e:
                # Skip malformed lines
                sys.stderr.write(f"Skipping malformed line: {line}\n")
                continue
        # Output results in readable format
        print("\n" + "="*60)
        print("WORD COUNT WITH STATISTICS - ANALYSIS RESULTS")
        print("="*60)
        # Sort words by frequency (descending) and output top 20
        print("\nWord Frequencies (Top 20):")
        print("-"*40)
        sorted_words = sorted(word_counts.items(), key=itemgetter(1), reverse=True)
        for word, count in sorted_words[:20]:
            print(f"{word}: {count}")
        # Output word length distribution
        print("\n\nWord Length Distribution:")
        print("-"*40)
        sorted_lengths = sorted(length_counts.items())
        for length, count in sorted_lengths:
            print(f"length_{length}: {count}")
        # Output statistics
        print("\n\nStatistics:")
        print("-"*40)
        print(f"Total words: {total_words}")
        print(f"Unique words: {len(word_counts)}")
        # Calculate average word length
        total_length_sum = sum(length * count for length, count in length_counts.items())
        avg_word_length = total_length_sum / total_words if total_words > 0 else 0
        print(f"Average word length: {avg_word_length:.2f}")
        print("="*60 + "\n")
    except Exception as e:
        sys.stderr.write(f"Reducer Error: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
