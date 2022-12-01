import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import mwparserfromhell
from lm_dataformat import Archive
import mwxml
import bz2
import shutil
import spacy
import json
import glob
import hashlib


def get_file_url_from_page(url, ext='', params={}):

    response = requests.get(url, params=params)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    parent = [url + node.get('href') for node in soup.find_all('a')
              if node.get('href').endswith(ext)]
    return parent


def download_file(url, dir):

    ok = True
    parts = url.split('/')
    file_name = parts[-1]

    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(os.path.join(dir, file_name), 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        ok = False

    return ok, file_name


def uncompress_file(file_name_bz2, dir):

    ok = True
    file_name = file_name_bz2[:-4]
    try:
        with bz2.BZ2File(os.path.join(dir,file_name_bz2)) as fr, open(os.path.join(dir,file_name),"wb") as fw:
            shutil.copyfileobj(fr, fw)
    except Exception as e:
        ok = False
        print(str(e))
    return ok, file_name


def correct(text):
    corrected = ""
    lines = text.splitlines()
    for line in lines:
        if not line.startswith("thumb|"):
            corrected += line.strip() + "\n"
    return corrected

def read_files(dir, file_name):

    meta = {}
    text = ""

    with open(os.path.join(dir, file_name), 'r') as f:
        text = f.read()

    meta_file_name = file_name[:-4] + '.meta'
    with open(os.path.join(dir, meta_file_name), 'r') as f:
        meta = json.load(f)

    return text, meta

def exits_files(dir, hash_name):
    
    dir1 = file_name[:1]
    dir2 = file_name[:2]

    new_dir = os.path.join(dir, dir1, dir2)

    if not os.path.exists(new_dir):
        return False

    if os.path.exists(os.path.join  (new_dir, hash_name + '.txt')) and os.path.exists(os.path.join(new_dir, hash_name + '.meta')):
        return True

    return False

def save_files(hash_name, txt, meta, dir):

    dir1 = hash_name[:1]
    dir2 = hash_name[:2]

    new_dir = os.path.join(dir, dir1, dir2)

    if not os.path.exists(new_dir):
        os.makedirs(new_dir, exist_ok=True)

    with open(os.path.join(new_dir, hash_name + '.txt'), 'w') as f:
        f.write(txt)

    with open(os.path.join(new_dir, hash_name + '.meta'), 'w') as f:
        json.dump(meta, f)

    return

def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0
    punctuations = 0
    symbols = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])
    punctuations = len([token.text for token in doc if (token.is_punct or token.pos_ == "PUNCT")])
    symbols = len([token.text for token in doc if (token.pos_ == "SYM")])

    return sentences, words, verbs, nouns, punctuations, symbols


base_dir = os.path.join(os.path.dirname(__file__))
cache_dir = os.path.join(base_dir, 'cache')
lm_dataformat_dir = os.path.join(base_dir, 'data')

if not os.path.exists(cache_dir):
    os.makedirs(cache_dir, exist_ok=True)

ar = Archive(lm_dataformat_dir)
url = 'https://dumps.wikimedia.org/plwiki/latest/'
ext = 'bz2'
file_name_zst = 'plwiki.jsonl.zst'
file_name_manifest = 'plwiki.manifest'
nlp = spacy.load("pl_core_news_md")

total_len = 0
total_docs = 0
total_sentences = 0
total_words = 0
total_verbs = 0
total_nouns = 0
total_punctuations = 0
total_symbols = 0

files = get_file_url_from_page(url, ext)
for f in files:
    if 'pages-articles-multistream' in f and 'xml-' in f:
        ok, file_name_bz2 = download_file(f, base_dir)
        if ok:
            print(f'Downloaded {file_name_bz2}')
            ok, file_name = uncompress_file(file_name_bz2, base_dir)
            if ok:
                print(f'Uncompressed {file_name}')
                dump = mwxml.Dump.from_file(open(os.path.join(base_dir,file_name)))
                for page in tqdm(dump, desc= 'Processing pages', unit=' page'):
                    for revision in page:
                        wikicode = mwparserfromhell.parse(revision.text)
                        txt = correct(wikicode.strip_code())
                        hash_name = hashlib.md5(txt.encode()).hexdigest()
                        if not exits_files(cache_dir, hash_name):
                            l = len(txt.strip())
                            sentences, words, verbs, nouns, punctuations, symbols = get_word_stats(txt)
                            total_words += words
                            total_verbs += verbs
                            total_nouns += nouns
                            total_len += l
                            total_docs += 1
                            total_sentences += sentences
                            total_punctuations += punctuations
                            total_symbols += symbols
                            save_files(hash_name, txt, {'title': page.title, 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols}, cache_dir)

                print(f'Parsed {file_name}')


            if os.path.exists(os.path.join(base_dir, file_name_bz2)):
                os.remove(os.path.join(base_dir, file_name_bz2))

            if os.path.exists(os.path.join(base_dir, file_name)):
                os.remove(os.path.join(base_dir, file_name))

        else:
            print(f'Error downloading {file_name_bz2}')


for x in os.walk(cache_dir):
    for y in glob.glob(os.path.join(x[0], '*.txt')):
        file_name_txt = os.path.basename(y)
        path_txt = os.path.dirname(y)
        txt, meta = read_files(path_txt, file_name_txt)
        if meta.get('sentences') > 5:
            ar.add_data(txt, meta=meta)
            print("Added " + meta.get('title'))

ar.commit()

data_files= glob.glob(lm_dataformat_dir + '/*')
file_size = 0

for f in data_files:
    if f.endswith('.zst'):
        shutil.copy(f, os.path.join(base_dir,file_name_zst))
        file_size = os.path.getsize(os.path.join(base_dir,file_name_zst))

    os.remove(f)

manifest = {"project" : "SpeakLeash", "name": "plwiki", "description": "Polish Wikipedia", "license": "CC-BY-SA 3.0", "language": "pl", "file_size" : file_size, "sources": [{"name": "plwiki", "url": "https://dumps.wikimedia.org/plwiki/latest/", "license": "CC-BY-SA 3.0"}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols}}
json_manifest = json.dumps(manifest, indent = 4) 

with open(os.path.join(base_dir, file_name_manifest), 'w') as mf:
    mf.write(json_manifest)
