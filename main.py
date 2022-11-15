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


def download_file(url):

    ok = True
    parts = url.split('/')
    file_name = parts[-1]

    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(file_name, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        ok = False

    return ok, file_name


def uncompress_file(file_name_bz2):

    ok = True
    file_name = file_name_bz2[:-4]
    try:
        with bz2.BZ2File(file_name_bz2) as fr, open(file_name,"wb") as fw:
            shutil.copyfileobj(fr, fw)
    except:
        ok = False
    return ok, file_name


def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])

    return sentences, words, verbs, nouns




ar = Archive('./data')
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

files = get_file_url_from_page(url, ext)
for f in files:
    if 'pages-articles-multistream' in f and 'xml-' in f:
        ok, file_name_bz2 = download_file(f)
        #ok = True
        #file_name_bz2 = "plwiki-latest-pages-articles-multistream1.xml-p1p187037.bz2"
        if ok:
            print(f'Downloaded {file_name_bz2}')
            ok, file_name = uncompress_file(file_name_bz2)
            if ok:
                print(f'Uncompressed {file_name}')
                dump = mwxml.Dump.from_file(open(file_name))
                for page in tqdm(dump, desc= 'Processing pages', unit=' page'):
                    for revision in page:
                        wikicode = mwparserfromhell.parse(revision.text)
                        txt = wikicode.strip_code()
                        l = len(txt.strip())
                        sentences, words, verbs, nouns = get_word_stats(txt)
                        total_words += words
                        total_verbs += verbs
                        total_nouns += nouns
                        total_len += l
                        total_docs += 1
                        total_sentences += sentences
                        ar.add_data(txt, meta={'length': l})

                    #if total_docs > 100:
                    #    break

                print(f'Parsed {file_name}')


            if os.path.exists(file_name_bz2):
                os.remove(file_name_bz2)

            if os.path.exists(file_name):
                os.remove(file_name)

            break

        else:
            print(f'Error downloading {file_name_bz2}')


ar.commit()

data_files= glob.glob('./data/*')
file_size = 0

for f in data_files:
    print(f)
    if f.endswith('.zst'):
        shutil.copy(f, file_name_zst)
        file_size = os.path.getsize(file_name_zst)

    os.remove(f)

manifest = {"project" : "SpeakLeash", "name": "plwiki", "description": "Polish Wikipedia", "license": "CC-BY-SA 3.0", "language": "pl", "file_size" : file_size, "sources": [{"name": "plwiki", "url": "https://dumps.wikimedia.org/plwiki/latest/", "license": "CC-BY-SA 3.0"}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len}}
json_manifest = json.dumps(manifest, indent = 4) 

with open(file_name_manifest, 'w') as mf:
    mf.write(json_manifest)


