with open('output_hl7/2025-03-03-16-21-27.7.hl7', 'rb') as f:
    data = f.read()
print([b for b in data])
