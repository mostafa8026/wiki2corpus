#!/usr/bin/python
#coding=utf-8

"""
MIT License

Copyright (c) 2017 Vit Baisa, Vit Suchomel, Marek Blahus

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
MediaWiki API help:
https://www.mediawiki.org/w/api.php?action=help&modules=query
"""

VERSION = '1.2.0'

import re
import os
import sys
import argparse
# use requests
import urllib2
import urllib
import httplib # httplib.HTTPException
import datetime
import json
import time
import gzip
from justext import core as justext
from lxml.etree import XMLSyntaxError, ParserError
from unicodedata import category as unicode_char_category

remove_links_re = re.compile(u'</?a[^>]*>')

class MissingPage(Exception):
    pass

class EmptyHTML(Exception):
    pass

class EmptyJusText(Exception):
    pass

LATEST = 'https://dumps.wikimedia.org/%swiki/latest/%swiki-latest-all-titles-in-ns0.gz'
WIKI_URL = 'https://%s.wikipedia.org/wiki/'
API_HTML = 'https://%s.wikipedia.org/w/api.php?action=parse&page=%s&format=json'
API_JSON = 'https://%s.wikipedia.org/w/api.php?action=query&prop=revisions&titles=%s&format=json'

# TODO: look at https://en.wikipedia.org/w/api.php?action=query&prop=extracts&format=json&titles=xxx

JUSTEXT_PARAMS_BY_LEVEL = {
    'verystrict': { #Justext default
        'length_low': 70,
        'length_high': 200,
        'stopwords_low': 0.3,
        'stopwords_high': 0.32,
        'max_link_density': 0.2,
        'max_good_distance': 5,
        'max_heading_distance': 150,
    },
    'strict': { #recommended
        'length_low': 70,
        'length_high': 200,
        'stopwords_low': 0.25,
        'stopwords_high': 0.32,
        'max_link_density': 0.3,
        'max_good_distance': 5,
        'max_heading_distance': 150,
    },
    'balanced': {
        'length_low': 55,
        'length_high': 140,
        'stopwords_low': 0.2,
        'stopwords_high': 0.3,
        'max_link_density': 0.4,
        'max_good_distance': 5,
        'max_heading_distance': 200,
    },
    'permissive': {
        'length_low': 40,
        'length_high': 90,
        'stopwords_low': 0.2,
        'stopwords_high': 0.3,
        'max_link_density': 0.45,
        'max_good_distance': 10,
        'max_heading_distance': 300,
    },
}

def html2prevert(s, justext_wordlist, justext_level='strict', allowshort=False):
    # TODO: preclean HTML (remove tables, infoboxes, References, TOC, ...)
    try:
        html_root = justext.preprocess(html_text=s, encoding='utf-8')
        paragraphs = justext.make_paragraphs(html_root)
    except (ParserError, XMLSyntaxError):
        return ('', 0, 0)
    #use Justext to classify paragraphs
    j_length_low = JUSTEXT_PARAMS_BY_LEVEL[justext_level]['length_low']
    j_length_high = JUSTEXT_PARAMS_BY_LEVEL[justext_level]['length_high']
    j_max_heading_distance = JUSTEXT_PARAMS_BY_LEVEL[justext_level]['max_heading_distance']
    if allowshort:
        j_length_low = j_length_low / 3
        j_length_high = j_length_high / 3
        j_max_heading_distance = j_max_heading_distance / 3
    justext.classify_paragraphs(
        paragraphs=paragraphs,
        stoplist=justext_wordlist,
        length_low=j_length_low, #character count < length_low => bad or short
        length_high=j_length_high, #character count > length_high => good
        stopwords_low=JUSTEXT_PARAMS_BY_LEVEL[justext_level]['stopwords_low'], #number of words frequent in the language >= stopwords_low => neargood
        stopwords_high=JUSTEXT_PARAMS_BY_LEVEL[justext_level]['stopwords_high'], #number of words frequent in the language >= stopwords_high => good or neargood
        max_link_density=JUSTEXT_PARAMS_BY_LEVEL[justext_level]['max_link_density'] #density of link words (words inside the <a> tag) > max_link_density => bad
    )
    justext.revise_paragraph_classification(
        paragraphs=paragraphs,
        max_heading_distance=j_max_heading_distance #Short/near-good heads in the distance [chars] before a good par => good
    )
    #extract good paragraphs
    prevert_paragraphs, paragraph_count, plaintext_len = [], 0, 0
    for p in paragraphs:
        #if p['class'] == 'good': # TODO find why this does not produce a good result
        if p['cfclass'] in ('good', 'neargood'): #'good', 'neargood', 'short', 'bad'
            p_text = justext.html_escape(p['text']).strip()
            if p_text:
                paragraph_count += 1
                plaintext_len += len(p_text)
                heading = u' heading="1"' if p['heading'] else u''
                prevert_paragraphs.append(u'<p%s>\n%s\n</p>' % (heading, p_text))
    return (u'\n'.join(prevert_paragraphs), paragraph_count, plaintext_len)

def api_wait(last, wait_interval):
    n = datetime.datetime.now()
    interval = (n-last).seconds + ((n-last).microseconds / 1.0e6)
    if interval < wait_interval:
        time.sleep(wait_interval - interval)

def process_page(langcode, title, linksf, raw_response_fp, justext_wordlist, logf,
        last_api_parse, wait_interval, justext_level, allowshort):
    api_wait(last_api_parse, wait_interval)
    api_url = API_HTML % (langcode, title)
    try:
        response_data = urllib2.urlopen(api_url).read()
        parse_time = datetime.datetime.now()
    except (IOError, httplib.HTTPException):
        # IOError includes both urllib2.URLError and socket.error (Python >= 2.6 for the latter)
        raise MissingPage()
    data = json.loads(response_data)
    if not data or 'error' in data:
        raise MissingPage()
    #store the API response to allow re-processing without downloading in the future
    raw_response_fp.write('%s\t%d\n' % (api_url, len(response_data) + 1))
    raw_response_fp.write(response_data)
    raw_response_fp.write('\n')
    #parse the API response
    p = data['parse']
    html = p['text']['*'].strip()
    if html:
        #remove <a/> tags (Justext makes extra spaces there) # TODO: correct Justext and remove
        html = remove_links_re.sub('', html)
        prevert, paragraph_count, plaintext_len = html2prevert(
            html.encode('utf-8'), justext_wordlist, justext_level, allowshort) # justext decodes!
    else:
        raise EmptyHTML()
    if not prevert:
        raise EmptyJusText()
    revid = p['revid']
    langlinks_len = len(p['langlinks'])
    categories = '|'.join([d['*'].replace('"', '') for d in p['categories']])
    if linksf and p['externallinks']:
        linksf.write('### %s\n' % title)
        for line in p['externallinks']:
            linksf.write(line.encode('utf-8') + '\n')
        linksf.write('\n')
    print >>logf, '\t%d chars' % plaintext_len
    page_attrs = 'url="%s" title="%s" categories="%s" translations="%d" paragraphs="%d" chars="%d" downloaded="%s"' % \
            ((WIKI_URL % langcode) + title, title, categories.encode('utf-8'),
            langlinks_len, paragraph_count, plaintext_len,
            parse_time.strftime('%Y-%m-%d %H:%M'))
    page = '<doc %s>\n%s\n</doc>\n' % (page_attrs, prevert.encode('utf-8'))
    return (page, paragraph_count, revid, parse_time)

def go_page(langcode, title, linksf, raw_response_fp, newest, justext_wordlist, logf, last_api_request,
        last_api_parse, wait_interval, justext_level, allowshort, cache, cf, hits_by_type):
    page, parlen, revid = '', 0, 0
    if title in cache: # if page has been found in cache
        if newest: # prepare to download the newest revision
            previous_revid = cache[title]
            api_wait(last_api_request, wait_interval)
            last_api_request = datetime.datetime.now()
            resp = urllib2.urlopen(API_JSON % (langcode, title))
            data = json.load(resp)
            dqp = data['query']['pages']
            for key in dqp.keys():
                try:
                    current_revid = dqp[key]['revisions'][0]['revid']
                except (KeyError, IndexError):
                    print >>logf, '\tusing old revision %s instead of the newest ' \
                        'revision (invalid Wiki API response data)' % previous_revid
                    current_revid = previous_revid
                if current_revid == previous_revid: # skip if cached is already newest
                    hits_by_type['skipped'] += 1
                    print >>logf, '\tskipping cached'
                    return (page, parlen, revid, last_api_parse)
        else: # skip because in cache
            hits_by_type['skipped'] += 1
            print >>logf, '\tskipping already downloaded'
            return (page, parlen, revid, last_api_parse)
    # download the page
    try:
        page, parlen, revid, last_api_parse =\
                process_page(langcode, title, linksf,
                raw_response_fp, justext_wordlist, logf,
                last_api_parse, wait_interval,
                justext_level, allowshort)
        cache[title] = revid
        cf.write('%s\t%s\n' % (title, revid))
        hits_by_type['processed'] += 1
        print >>logf, '\t%d paragraphs' % parlen
    except (MissingPage, EmptyHTML, EmptyJusText) as e:
        page = ''
        hits_by_type['empty'] += 1
        print >>logf, {
                'MissingPage': '\tempty because not found',
                'EmptyHTML': '\tempty HTML parse returned by API',
                'EmptyJusText': '\tempty prevert returned by jusText'} \
                [type(e).__name__]
    return (page, parlen, revid, last_api_parse)

def main(langcode, cachefn, raw_response_path, newest, links, justext_wordlist,
        logf, wait_interval, nicetitles, talkpages, justext_level, allowshort):
    last_api_request = datetime.datetime.now()
    last_api_parse   = datetime.datetime.now()

    linksf = open(links, 'a') if links else None
    cache = {}
    if os.path.exists(cachefn):
        print >>logf, 'Cache: %s' % cachefn
        with open(cachefn) as cf:
            for line in cf:
                try:
                    title, revid = line.split('\t')
                    cache[title.strip()] = revid.strip()
                except ValueError:
                    continue
    cf = open(cachefn, 'a') # cache file
    raw_response_fp = open(raw_response_path, 'a') #raw API responses
    wikidump_titles_path = LATEST % (langcode.replace('-', '_'), langcode.replace('-', '_'))
    print >>logf, 'Getting all titles from latest Wikipedia dump %s' % wikidump_titles_path
    hits_by_type = {'processed': 0, 'skipped': 0, 'empty': 0}
    filename, _ = urllib.urlretrieve(wikidump_titles_path)
    with gzip.open(filename) as df:
        for line in df:
            title = line.strip().replace('"', "'")
            # TODO: filter titles, use RE as parameter
            print >>logf, '%s' % title
            if nicetitles and not unicode_char_category(unicode(title, 'utf-8')[0])[0] == 'L':
                print >>logf, '\tskipping (not a nice title)', title
                continue
            for page_title in filter(None, [title, 'Talk:' + title if talkpages else None]):
                page, parlen, revid, last_api_parse =\
                        go_page(langcode, page_title, linksf,
                        raw_response_fp, newest, justext_wordlist, logf,
                        last_api_request, last_api_parse, wait_interval,
                        justext_level, allowshort, cache, cf, hits_by_type)
                if page:
                    sys.stdout.write(page)
    print >>logf, 'Updated cache database stored in %s' % cachefn
    if linksf:
        linksf.close()
    cf.close()
    for hit_type, hit_count in hits_by_type.items():
        print >>logf, '%s: %d' % (hit_type.title(), hit_count)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Wikipedia downloader')
    parser.add_argument('langcode', help='Wikipedia language prefix', type=str)
    parser.add_argument('wordlist', help='Path to a list of ~2000 most frequent words in the language (UTF-8, one per line)', type=str)
    parser.add_argument('--cache', help='File with previously downloaded pages and data', type=str, default='')
    parser.add_argument('--wait', help='Time interval between GET requests', type=float, default=1.0)
    parser.add_argument('--newest', help='Download the newest versions of articles', action='store_true')
    parser.add_argument('--links', help='Gather external links from Wikipedia', type=str, default='')
    parser.add_argument('--nicetitles', help='Download only titles starting with alphabetical character', action='store_true')
    parser.add_argument('--talkpages', help='Download talk pages', action='store_true')
    parser.add_argument('--cleaning', help='Level of Justext boilerplate & short paragraph removal strictness (default = strict)',
        type=str, choices=('verystrict', 'strict', 'balanced', 'permissive'), default='strict')
    parser.add_argument('--allowshort', help='Allow three times shorter texts. Useful for ideographic scripts.', action='store_true')
    args = parser.parse_args()
    cachefile = args.cache or args.langcode + 'wiki.cache'
    raw_response_file = (args.cache or args.langcode) + '_raw_data'
    with open(args.wordlist) as fp:
        justext_wordlist = set([line.decode('utf-8').rstrip() for line in fp])
    logfn = args.langcode.replace('/','') + '_' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
    with open(logfn, 'w', buffering=1) as logfile:
        main(args.langcode, cachefile, raw_response_file, args.newest, args.links,
            justext_wordlist, logfile, args.wait, args.nicetitles, args.talkpages,
            args.cleaning, args.allowshort)
