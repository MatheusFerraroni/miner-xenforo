import argparse
import json
import os
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
import sys
import pandas as pd
import threading
import time
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import nltk
import datetime

class Cleaner:

    def __init__(self, url, min_l, max_l, conversations, cache, threads, only_empty_msgs):
        self.base_url = url
        self.domain = urlparse(self.base_url).netloc
        self.min = min_l
        self.max = max_l
        self.conversations = conversations
        self.cache = cache
        self.max_threads = threads
        self.tokenizer = nltk.tokenize.TweetTokenizer()
        self.only_empty_msgs = only_empty_msgs

        self.cache_identify_conversations = {}

        # folders to work
        self.config_folder     = "./config/{}/".format(self.domain) # must exist
        self.threads_folder    = self.config_folder+"threads/" # must exist
        self.result_folder     = self.config_folder+"clear_threads/" # must be created
        self.plots_folder     = self.config_folder+"plots/" # must be created
        self.clear_cache_file  = self.config_folder+"clear_cache.csv"

        # locks
        self.lock_alter_infos = threading.Lock()


        # extras
        self.punctuation = ['!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ',', '-', '.', ':', ';', '=', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~']

        self.create_configs()

    def create_configs(self):

        if not os.path.exists(self.config_folder):
            raise Exception("Config folder not found: {}".format(self.config_folder))

        if not os.path.exists(self.threads_folder):
            raise Exception("Thread folder not found: {}".format(self.threads_folder))

        if not os.path.exists(self.result_folder): # create config folder
            print("Creating folder: {}".format(self.result_folder))
            os.makedirs(self.result_folder)

        if not os.path.exists(self.plots_folder): # create config folder
            print("Creating folder: {}".format(self.plots_folder))
            os.makedirs(self.plots_folder)


        self.tags = {
            "external_image": " <image> ",
            "emoji": " <emoji> {} </emoji> ",
            "img_unknown": " <image_unknown> ",
            "mediaembed": " <mediaembed> {} </mediaembed> ",
            "url": " <url> {} {} </url> ", # <a href='k.com/1/2/3'> abc </a> = <url> k.com abc </url>
            "link": " <link> ", # https://www.website.com/getparameter.... = <link>
            "shared_content": " <shared_content> {} {} </shared_content> ",
            "quote": " <quote> {} </quote> ",
            "answering": " <answering> {} </answering> ",
            "spoiler": " <spoiler> {} </spoiler> ",
            "code": " <code> {} </code> ",
            "iframe": " <iframe> {} </iframe> ",
        }

    def load_infos(self):

        print("Using cache: {}".format(self.cache))

        load_all = True
        if self.cache:
            print("Cache file: {}".format(self.clear_cache_file))
            print("Cache file found: {}".format(os.path.isfile(self.clear_cache_file)))
            if os.path.isfile(self.clear_cache_file):
                print("Loading cache files")
                self.infos = pd.read_csv(self.clear_cache_file, sep="\t")
                print("Cache files loaded")
                load_all = False

        if load_all:
            print("Reading threads from {}".format(self.threads_folder))
            files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(self.threads_folder) for f in filenames if os.path.splitext(f)[1] == '.json']

            dats = []
            ids = []
            categories = []
            subcategories = []
            total_messages = []
            dates = []
            updates = []
            for f in files:
                with open(f, "r") as f:
                    dat = json.loads(f.read())
                    dats.append({
                        "id": dat['id'],
                        "category": dat['category'],
                        "subcategory": dat['subcategory'],
                        "total_messages": len(dat['messages']),
                        "date_thread": dat['date_thread'],
                        "last_update": dat['last_update'],
                        })
                    ids.append(dat['id'])
                    categories.append(dat['category'])
                    subcategories.append(dat['subcategory'])
                    total_messages.append(len(dat['messages']))
                    dates.append(dat['date_thread'])
                    updates.append(dat['last_update'])

            self.infos = pd.DataFrame({
                'id': ids,
                'category': categories,
                'subcategory': subcategories,
                'total_messages': total_messages,
                'date_thread': dates,
                'last_update': updates,
                })

            self.save_infos()

    def limpar_post(self, post_bs):
        if type(post_bs)==str:
            post_bs = BeautifulSoup(post_bs, features="lxml")

        for el in post_bs.find_all('div', class_="kl_amdp_merge_message"): # remove mensagem de post duplo
            el.extract()

        iframes = post_bs.find_all('iframe')
        for iframe in iframes: # detecta iframes
            domain = urlparse(iframe['src']).netloc
            insert = self.tags['iframe'].format(domain)
            iframe.insert_after(insert)
            iframe.extract()


        bbcodeblockscode = post_bs.find_all('div', class_="bbCodeBlock--code")
        for bbcodeblockcode in bbcodeblockscode: # detecta codes
            insert = self.tags['code'].format(self.to_single_line(bbcodeblockcode.find('code').text))
            bbcodeblockcode.insert_after(insert)
            bbcodeblockcode.extract()


        bbcodeblocks = post_bs.find_all('div', class_="bbCodeBlock")
        for bbcodeblock in bbcodeblocks: # detecta shared contents
            ok = False
            try:
                data_host = bbcodeblock['data-host']

                title = bbcodeblock.find_all('a')[0].getText()
                ok = True
            except:
                pass

            if ok:
                insert = self.tags['shared_content'].format(data_host, title)

                bbcodeblock.insert_after(insert)

                bbcodeblock.extract()

        for img in post_bs.find_all('img'): # detecta images
            overwrite_with = ""
            if 'bbImage' in img['class']:
                overwrite_with = self.tags['external_image']
            elif 'smilie' in img['class']:
                overwrite_with = self.tags['emoji'].format(img['alt'])
            else:
                overwrite_with = self.tags['img_unknown']

            img.insert_after(overwrite_with)
            img.unwrap()

        for a in post_bs.find_all('a'): # detecta links

            try:
                text = a.getText()
                domain = urlparse(a['href']).netloc
                text = re.sub('(http:\/\/\S+|https:\/\/\S+)', self.tags['link'], text, flags=re.MULTILINE)
                insert = self.tags['url'].format(domain, text)
                a.insert_after(insert)
            except:
                pass

            a.extract()

        for span in post_bs.find_all('span'): # detect videos
            try:
                source = span['data-s9e-mediaembed']

                insert = self.tags['mediaembed'].format(source)

                span.insert_after(insert)

                span.extract()
            except:
                pass

        quotes = post_bs.find_all('blockquote') # remove quotes
        quotes = quotes[::-1]
        for quote in quotes:
            title = quote.find('div', class_="bbCodeBlock-title")
            content = quote.find('div', class_="bbCodeBlock-content")
            if title==None:
                insert = self.tags['quote'].format(self.limpar_post(content))

                quote.insert_after(insert)
            quote.extract() # THIS WILL REMOVE ALL QUOTES AFTER IDENTIFIED

        spoilers = post_bs.find_all('div', class_='bbCodeSpoiler')
        spoilers = spoilers[::-1]
        for spoiler in spoilers:
            insert = self.tags['spoiler'].format(self.limpar_post(spoiler))
            spoiler.insert_after(insert)
            spoiler.extract()

        res = post_bs.text
        res = re.sub(r'\n', ' ', res)
        res = re.sub(r' +', ' ', res)
        res = re.sub(u"\u200b", ' ', res)
        res = re.sub(u'\xa0', ' ', res)
        return res

    def identify_conversations(self, msg, thread_id, msg_id):
        try:
            return self.cache_identify_conversations[thread_id][msg_id] # cache mechanism
        except:
            pass
        
        msg = BeautifulSoup(msg, features="lxml")

        quotes = msg.find_all("blockquote", class_="bbCodeBlock--quote")
        res = []

        for quote in quotes:
            title = msg.find_all("div", class_="bbCodeBlock-title")
            for t in title:
                if t.a!=None:
                    res.append(t.a['data-content-selector'])

        res = list(set(res))
        res.sort()
        
        self.cache_identify_conversations[thread_id][msg_id] = res
        return res

    def mount_conversation(self, thread_id, orig, dat, index_message, res_final):
        try:
            index_message["#"+orig['official_id']]
        except:
            return res_final

        cs = self.identify_conversations(orig['message'], thread_id, orig['official_id'])

        try:
            orig['parent']
        except:
            orig['parent'] = None
        
        for c in cs:
            
            check_repeated = orig
            can_continue = True
            while check_repeated!=None:
                if c=="#"+check_repeated['official_id']:
                    can_continue = False
                    break
                check_repeated = check_repeated['parent']
            
            if can_continue:
                try:
                    idx = index_message[c]
                    next_message = dat['messages'][idx]
                except:
                    continue
                if next_message['isodate']<orig['isodate']: # avoid timetravel response
                    next_message['parent'] = orig
                    res_final = self.mount_conversation(thread_id, next_message, dat, index_message, res_final)

        if len(cs)==0:
            r = []
            while orig['parent']!=None:
                idx = index_message["#"+orig['official_id']]
                r.append(dat['messages'][idx])
                orig = orig['parent']

            idx = index_message["#"+orig['official_id']]
            r.append(dat['messages'][idx])

            if len(r)>1:
                res_final.append(r)

        return res_final


    def to_single_line(self, s):
        s = s.replace("\r", " ")
        return s.replace("\n", " ")

    def do_process(self, th):
        try:
            dat = None
            with open(self.threads_folder+"{}.json".format(th['id']), 'r') as f:
                dat = f.read()
                f.close()
                dat = json.loads(dat)

            index_message = {}

            for i in range(len(dat['messages'])):
                idd = "#{}".format(dat['messages'][i]['official_id'])
                index_message[idd] = i

                isodate = dat['messages'][i]['creation']
                isodate = isodate[0:22]+":"+isodate[22:]
                dat['messages'][i]['isodate'] = datetime.datetime.fromisoformat(isodate)

            tokens_lens = []
            for i in range(0, len(dat['messages']), 1):
                dat['messages'][i]['message_clear'] = self.limpar_post(dat['messages'][i]['message'])

                txt = dat['messages'][i]['message_clear']+""
                for p in self.punctuation:
                    txt = txt.replace(p, " ")

                tokens_lens.append(len(self.tokenizer.tokenize(txt)))



            with open(self.result_folder+"{}.tsv".format(th['id']), 'w') as f:
                for i in range(0, len(dat['messages']), 1):
                    txt = ''
                    if i>0:
                        txt += "\n"
                    txt += "{}\t{}\t{}".format(dat['messages'][i]['creation'], dat['messages'][i]['user_href'], dat['messages'][i]['message_clear'])
                    f.write(txt)


            conversations_lens = []
            counter_conversation = -1
            if self.conversations:
                self.cache_identify_conversations[th['id']] = {}
                for i in range(len(dat['messages'])-1, -1, -1):
                    post = dat['messages'][i]

                    convs = self.mount_conversation(th['id'], post, dat, index_message, []) # identify who this post is replying and which part

                    for c in convs:
                        conversations_lens.append(len(c))
                        counter_conversation += 1

                        with open(self.result_folder+"{}_{}.tsv".format(th['id'], counter_conversation), 'w') as f:
                            txt = ''
                            for ii in range(len(c)):
                                if ii>0:
                                    txt += "\n"
                                cc = c[ii]
                                txt += "{}\t{}\t{}".format(cc['creation'], cc['user_href'], cc['message_clear'])

                            f.write(txt)


                    for conv in convs:
                        for m in conv:
                            try:
                                del( index_message["#"+m['official_id']] )
                            except:
                                continue
                self.cache_identify_conversations[th['id']] = {}

            self.set_infos(th, "tokens_lens", tokens_lens)
            self.set_infos(th, "conversations_lens", conversations_lens)
        except Exception as e:
            print("\n\n")
            print(e)
            print("\n\n")

    def set_infos(self, th, key, value):
        self.lock_alter_infos.acquire()

        try:
            if not key in self.infos.columns:
                self.infos[key] = [None]*len(self.infos)

            self.infos.loc[self.infos['id']==th['id'], key] = json.dumps(value)

        except Exception as e:
            print(e)

        self.lock_alter_infos.release()

    def save_infos(self):
        self.lock_alter_infos.acquire()
        self.infos.to_csv(self.clear_cache_file, sep='\t', index=False)
        self.lock_alter_infos.release()

    def process(self):

        sub = self.infos[self.infos["total_messages"] > self.min]
        sub = sub[sub["total_messages"] <= self.max]

        if self.only_empty_msgs==True:
            sub = sub[sub["conversations_lens"] == "[]"]

        save_every = len(sub)//200 # to save every 0.5%
        save_every = max(1, save_every)

        threads_running = []
        for i in range(len(sub)):
            th = sub.iloc[i]
            print("id={}\t{}/{} = {}%".format( th.id, i+1, len(sub), round((i/float(len(sub)))*100,1) ) )#, end='\r')


            x = threading.Thread(target=self.do_process, args=(th,))
            threads_running.append(x)
            x.start()

            while True:
                threads_running = [thread for thread in threads_running if thread.is_alive()]

                if len(threads_running)<self.max_threads:
                    break
                time.sleep(0.01)

            if i%save_every==0:
                self.save_infos()

        print("Main loop complete. Waiting for final threads")
        for x in threads_running:
            x.join()

        self.save_infos()

    def plots(self):

        sub = self.infos[self.infos["total_messages"] > self.min]
        sub = sub[sub["total_messages"] <= self.max]


        sns.set_theme(style="ticks")

        f, ax = plt.subplots(figsize=(7, 6))

        ax.set_yscale("log")

        box_plot = sns.boxplot(data=sub['total_messages'])

        # box_plot.axes.axhline(sub['total_messages'].mean(), ls='--')

        median = sub['total_messages'].median()
        q1 = sub["total_messages"].quantile(0.25)
        q3 = sub["total_messages"].quantile(0.75)
        iqr = q3 - q1
        x = sub['total_messages']
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)
        upper = max(x[x<upper])
        lower = min(x[x>lower])


        vals_show = []
        vals_show.append(q3)
        vals_show.append(q1)
        vals_show.append(median)
        vals_show.append(upper)
        vals_show.append(lower)
        vals_show.append(sub["total_messages"].max())

        for xtick in box_plot.get_xticks():
            for v in vals_show:
                v = v
                off = v * 0.1
                text = box_plot.text(
                                    xtick,
                                    v + off,
                                    v,
                                    horizontalalignment='center',
                                    size='x-small',
                                    color='white',
                                    weight='semibold',
                                )

                text.set_path_effects([
                    path_effects.Stroke(linewidth=3, foreground="grey"),
                    path_effects.Normal(),
                ])


        ax.yaxis.grid(True)
        box_plot.set(xticklabels=[])
        ax.set_xlabel("Number of threads")
        ax.set_ylabel("Size of thread")
        box_plot.set(title="Number of posts per thread")
        plt.savefig(self.plots_folder+"boxplot_numberpost_per_thread.pdf", bbox_inches='tight', pad_inches=0)
        plt.close()





        unique, counts = np.unique(sub['total_messages'], return_counts=True)

        sns.set_theme(style="ticks")
        fig, ax = plt.subplots(figsize=(7, 6))

        ax.set_yscale("log")

        line1, = ax.plot(counts, unique)

        ax.set_xlabel("Number of threads")
        ax.set_ylabel("Size of thread")
        ax.set_title("Number of posts per thread")
        ax.grid()

        plt.savefig(self.plots_folder+"line_numberpost_per_thread.pdf", bbox_inches='tight', pad_inches=0)
        plt.close()

def main(url, min_l, max_l, conversations, cache, threads, plots, only_empty_msgs):

    cleaner = Cleaner(url, min_l, max_l, conversations, cache, threads, only_empty_msgs)
    cleaner.load_infos()

    if plots:
        cleaner.plots()
    else:
        cleaner.process()

if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument("-url", required=True, type=str, help="The base URL of the forum using XenForo")
    ap.add_argument("-min", required=False, type=int, help="Min amount of posts to process thread", default=0)
    ap.add_argument("-max", required=False, type=int, help="Max amount of posts to process thread", default=float('inf'))
    ap.add_argument("-c", "--conversations", required=False, action="store_true", help="If present, all conversations will generate a different file")
    ap.add_argument("-ca", "--cache", required=False, action="store_true", help="If present, cache file will be created and used")
    ap.add_argument("-t", "--threads", required=False, type=int, help="Total of threads to create", default=1)
    ap.add_argument("-p", "--plots", required=False, action="store_true", help="If present, the plots will be generated. No processing is done")
    ap.add_argument("-oem", "--only_empty_msgs", required=False, action="store_true", help="Only process threads where conversations_lens=='[]'")

    args = vars(ap.parse_args())

    main(args['url'], args['min'], args['max'], args['conversations'], args['cache'], args['threads'], args['plots'], args['only_empty_msgs'])