import mmap
import os
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# Parses a line in
def parse_line(line):
    city, temp = line.strip().split(b";")       # split line at ";"
    return city.decode(), float(temp)           # return the city and temp for this line

# Process a chunk of the file using memory-mapping
def process_chunk_mmap(file_path, start, end):
    cities = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])  # defaultdict containing default values (lamda function to set positive, negative float numbers)

    with open(file_path, 'rb') as f:            # open the file
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)  # create a memory mapped object

        # Align to the front of the chunk, if not alredy there
        if start != 0:
            while mm[start - 1:start] != b'\n' and start < end:
                start += 1

        pos = start
        while pos < end:
            # Find the next newline
            line_end = mm.find(b'\n', pos, end)
            if line_end == -1:
                break  # Reached end of chunk
            line = mm[pos:line_end]
            pos = line_end + 1  # Move to start of next line

            city, temp = parse_line(line)
            stats = cities[city]
            stats[0] = min(stats[0], temp)
            stats[1] = max(stats[1], temp)
            stats[2] += temp
            stats[3] += 1

    return dict(cities)         # return cities (casted to dictionary)

# Merge results from all workers
def merge_stats(all_stats):
    merged = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])     # another defaultdict to store merged chunk values
    for stats in all_stats:
        for city, s in stats.items():
            m = merged[city]
            m[0] = min(m[0], s[0])
            m[1] = max(m[1], s[1])
            m[2] += s[2]
            m[3] += s[3]
    return merged

# Save final results to file
def write_results(cities, output_path="results.txt"):
    print(f"Writing results to {output_path}...")
    with open(output_path, "w", encoding="utf-8") as f:
        for city in sorted(cities.keys()):
            stats = cities[city]
            avg = int(stats[2] / stats[3]) 
            f.write(f"{city}={stats[0]}/{avg}/{stats[1]}\n")

def main(file_path, num_workers=None):
    num_workers = num_workers or cpu_count()  # Use all available cores
    file_size = os.path.getsize(file_path)    # Get size of the file in bytes
    chunk_size = file_size // num_workers     # Divide file into N equal byte chunks

    chunks = []
    for i in range(num_workers):
        start = i * chunk_size
        end = file_size if i == num_workers - 1 else (i + 1) * chunk_size
        chunks.append((file_path, start, end))

    # Process all chunks in parallel using a pool of available workers
    with Pool(processes=num_workers) as pool:
        results = pool.starmap(process_chunk_mmap, chunks)

    final_stats = merge_stats(results)
    write_results(final_stats, output_path="results.txt")

if __name__ == "__main__":
    main("measurements_1m.txt") 
