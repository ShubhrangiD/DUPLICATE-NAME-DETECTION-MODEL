# Duplicate Name Detection

This project focuses on detecting similar names within a dataset using various string similarity measures and parallel processing techniques. The primary goal is to identify potential duplicates in a dataset by leveraging phonetic encoding and string similarity functions.

## Table of Contents

1. [Import Libraries](#import-libraries)
2. [Preprocess the Name](#preprocess-the-name)
3. [Calculate Similarity Score](#calculate-similarity-score)
4. [Block Names](#block-names)
5. [Process Chunk of Names](#process-chunk-of-names)
6. [Process Blocks with Threads](#process-blocks-with-threads)
7. [Process All Blocks in Parallel](#process-all-blocks-in-parallel)
8. [Consolidate Similar Names into Clusters](#consolidate-similar-names-into-clusters)
9. [Sample Output](#sample-output)
10. [Contributing](#contributing)
11. [License](#license)

## Import Libraries

The script uses several libraries for data manipulation, phonetic encoding, and parallel processing:

```python
import pandas as pd  # for handling data frames.
from jellyfish import soundex, metaphone, levenshtein_distance, jaro_winkler_similarity  # for phonetic encoding and string similarity functions.
from collections import defaultdict  # for creating dictionaries with default values.
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  # for parallel processing.
import itertools  # for efficient looping and combinations.
```

## Preprocess the Name

This function preprocesses names by stripping leading/trailing spaces and converting them to lowercase:

```python
def preprocess_name(name: str) -> str:
    return name.strip().lower() if isinstance(name, str) else ''
```

## Calculate Similarity Score

This function calculates a similarity score between two names using various phonetic and string similarity measures:

```python
def get_similarity_score(name1, name2):
    name1, name2 = preprocess_name(name1), preprocess_name(name2)  # Preprocess both names.
    
    # Phonetic scores
    soundex_score = soundex(name1) == soundex(name2)
    metaphone_score = metaphone(name1) == metaphone(name2)
    
    # String similarity scores
    levenshtein_score = levenshtein_distance(name1, name2)
    jaro_winkler_score = jaro_winkler_similarity(name1, name2)
    
    # Combine these scores with weights to get a final similarity score.
    score = (soundex_score * 0.2 +
             metaphone_score * 0.2 +
             (1 - levenshtein_score / max(len(name1), len(name2))) * 0.3 +
             jaro_winkler_score * 0.3)
    
    return score
```

## Block Names

This function groups names by their first letter to reduce the number of comparisons:

```python
def block_names(df: pd.DataFrame) -> dict[str, list[str]]:
    blocks = defaultdict(list)
    for _, row in df.iterrows():
        first_letter = row['name'][0] if row['name'] else ''
        blocks[first_letter].append(row['name'])
    return blocks
```

## Process Chunk of Names

This function processes a chunk of names and identifies potential duplicates based on a similarity threshold:

```python
def process_chunk(chunk: list[str], threshold: float = 0.8) -> dict[str, list[str]]:
    potential_duplicates = defaultdict(list)
    seen_pairs = set()

    for name1, name2 in itertools.combinations(chunk, 2):
        if name1 > name2:
            name1, name2 = name2, name1
        
        pair = (name1, name2)

        if pair not in seen_pairs:
            score = get_similarity_score(name1, name2)
            if score > threshold and score < 1.00:
                potential_duplicates[name1].append(f"{name2} ({score:.2f})")
                potential_duplicates[name2].append(f"{name1} ({score:.2f})")
            seen_pairs.add(pair)

    return potential_duplicates
```

## Process Blocks with Threads

This function processes blocks of names in parallel using threads to speed up the comparison process:

```python
def process_block_with_threads(names: list[str], threshold: float = 0.8, chunk_size: int = 1000) -> dict[str, list[str]]:
    potential_duplicates = defaultdict(list)
    with ThreadPoolExecutor() as executor:
        futures = []
        for i in range(0, len(names), chunk_size):
            chunk = names[i:i + chunk_size]
            futures.append(executor.submit(process_chunk, chunk, threshold))

        for future in futures:
            result = future.result()
            for key, value in result.items():
                potential_duplicates[key].extend(value)

    return potential_duplicates
```

## Process All Blocks in Parallel

This function processes all blocks in parallel using multiple processes to further speed up the computation:

```python
all_potential_duplicates = defaultdict(list)
with ProcessPoolExecutor() as executor:
    futures = []
    for names in blocked_org_members.values():
        futures.append(executor.submit(process_block_with_threads, names))

    for future in futures:
        result = future.result()
        for key, value in result.items():
            all_potential_duplicates[key].extend(value)
```

## Consolidate Similar Names into Clusters

This part combines similar names into clusters and ensures each name is only processed once:

```python
clusters = []
visited = set()

for key, similars in all_potential_duplicates.items():
    if key not in visited:
        cluster = [key] + similars
        unique_cluster = sorted(set(cluster), key=str.lower)  # Remove duplicates and sort alphabetically
        visited.update(preprocess_name(name.split(' (')[0]) for name in unique_cluster)  # Update with base name
        clusters.append(unique_cluster)

# Sort clusters alphabetically by name
clusters.sort(key=lambda x: x[0])

# Create DataFrame for clusters
clustered_df = pd.DataFrame(clusters)

# Rename columns
max_cols = clustered_df.shape[1]
column_names = ['name'] + [f'duplicate_{i}' for i in range(1, max_cols)]
clustered_df.columns = column_names

# Save clusters to a CSV file
clustered_df.to_csv('DUPLICATE_NAMES.csv', index=False)
```

## Sample Output

The output CSV file, `DUPLICATE_NAMES.csv`, will contain clusters of similar names, with each row representing a primary name and its potential duplicates. Here’s an example of what the output might look like:

| name         | duplicate_1      | duplicate_2   |
|--------------|------------------|---------------|
| john smith   | john smyth (0.85)| jon smith (0.78)|
| jane doe     | jayne doe (0.82) | jan doe (0.79) |

## Contributing

Contributions are welcome! If you have any improvements, bug fixes, or suggestions, please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature/your-feature`).
6. Create a new Pull Request.

Please ensure your code adheres to the project's coding style and includes appropriate tests where applicable.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
