import json

# Load data from the file named bcsample.json
with open('bcsample.json') as f:
    # return JSON object as a dictionary
    bcsample_data = json.load(f)

# print first 10 records to verify we have data in actual file
print(bcsample_data[:10])

# save 1000 records from original bcsample into
# a new file named bcsample_1000.json
with open('bcsample_1000.json', 'w', encoding='utf-8') as f:
    json.dump(bcsample_data[:1000], f, ensure_ascii=False, indent=4)
