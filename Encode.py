import os

dir = 'results'

for filename in os.listdir(dir):
    if filename.endswith(".csv"):
        file = os.path.join(dir, filename)
        s = open(file, mode='r', encoding='utf-8').read()
        open(file, mode='w', encoding='utf-8-sig').write(s)
        continue
