import json

# Load data from the file named bcactual.json
with open('bcactual.json') as f:
    # return JSON object as a dictionary
    bcactual_data = json.load(f)

# print first 10 records to verify we have data in actual file
print(bcactual_data[:10])

# save 1000 records from original bcactual into
# a new file named bcsample.json
with open('bcsample.json', 'w', encoding='utf-8') as f:
    json.dump(bcactual_data[:1000], f, ensure_ascii=False, indent=4)
