#!/usr/bin/env python3
"""Scrape Ruh al-Ma'ani (Shamela book 22835) page by page into pages.jsonl.

Each line: {id, vol, pg, sec, surah, a1, a2, paras: [...]}
Resumable: re-run to fill in any pages missing from the output file.
"""
import json, re, html, os, sys, time, random
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

BOOK = 22835
LAST = 6473
OUT = 'pages.jsonl'
UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0 Safari/537.36')

AR_DIGITS = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')
SEC_RE = re.compile(
    r'\[سورة\s+(.+?)\s*\((\d+)\)\s*:\s*الآي(?:ات|ة)\s*(\d+)(?:\s*إلى\s*(\d+))?\]')


def fetch(url, tries=4):
    for n in range(tries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(2 ** n + random.random())
        except Exception:
            time.sleep(2 ** n + random.random())
    return None


def parse(pid, s):
    out = {'id': pid}

    t = re.search(r'<title>(.*?)</title>', s, re.S)
    if t:
        m = re.search(r'ج(\d+)\s*-\s*ص(\d+)', t.group(1))
        if m:
            out['vol'], out['pg'] = int(m.group(1)), int(m.group(2))

    # Current section: the TOC entry marked active on this page
    m = re.search(r'<a class="active"[^>]*>\s*(?:<span[^>]*>)?\s*(\[سورة[^<]+?\])', s)
    if not m:
        m = re.search(r'<span class="text-black">(\[سورة[^<]+?\])</span>', s)
    if m:
        sec = html.unescape(m.group(1)).strip()
        out['sec'] = sec
        sm = SEC_RE.search(sec.translate(AR_DIGITS))
        if sm:
            out['surah'] = int(sm.group(2))
            out['a1'] = int(sm.group(3))
            out['a2'] = int(sm.group(4) or sm.group(3))

    # Body
    i = s.find('class="nass')
    if i < 0:
        out['paras'] = []
        return out
    j = s.find('id="appended_pages"', i)
    body = s[i:j] if j > 0 else s[i:]
    body = body[body.find('>') + 1:]
    # Drop the trailing nav/toolbar that follows the last </p>
    k = body.rfind('</p>')
    if k > 0:
        body = body[:k + 4]
    body = re.sub(r'<a [^>]*btn_tag.*?</a>', '', body, flags=re.S)
    body = re.sub(r'<span[^>]*class="anchor"[^>]*></span>', '', body)

    paras = []
    for pm in re.finditer(r'<p[^>]*>(.*?)</p>', body, re.S):
        txt = re.sub(r'<br\s*/?>', '\n', pm.group(1))
        txt = html.unescape(re.sub(r'<[^>]+>', '', txt))
        txt = re.sub(r'[ \t]+', ' ', txt).strip()
        if txt:
            paras.append(txt)
    out['paras'] = paras
    return out


def main():
    done = set()
    if os.path.exists(OUT):
        with open(OUT, encoding='utf-8') as f:
            for line in f:
                try:
                    done.add(json.loads(line)['id'])
                except Exception:
                    pass
    todo = [p for p in range(1, LAST + 1) if p not in done]
    print(f'{len(done)} done, {len(todo)} to fetch', flush=True)

    lock = __import__('threading').Lock()
    fh = open(OUT, 'a', encoding='utf-8')
    n = [0]

    def work(pid):
        s = fetch(f'https://shamela.ws/book/{BOOK}/{pid}')
        rec = parse(pid, s) if s else {'id': pid, 'paras': [], 'err': 1}
        with lock:
            fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
            n[0] += 1
            if n[0] % 100 == 0:
                fh.flush()
                print(f'{n[0]}/{len(todo)}', flush=True)
        time.sleep(0.15)

    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(work, todo))
    fh.close()
    print('done', flush=True)


if __name__ == '__main__':
    main()
