
from urllib.parse import urlparse
import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import dateutil
import threading
import random
import time
import logging
import urllib.parse
import pytz
import sys
import time

def default(o): # to save custom format in json
    if type(o) is datetime:
        return o.isoformat()

class Base:

    def __init__(self, cache_pages):
        self.cache_pages = cache_pages
        self.base_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
        }
        logging.info("Class Base created")



    def warm_up(self):
        logging.info("Warming up Base with cache: {}".format(self.cache_pages))
        if self.cache_pages:
            self.cache_html_location = self.config_folder+"cache_html/"
            logging.info("Cache location: {}".format(self.cache_html_location))
            if not os.path.exists(self.cache_html_location): # create config folder
                os.makedirs(self.cache_html_location)


    def get_html_protected(self, url): # get html with cache
        logging.info("Will request URL: {}".format(url))

        file_name = None
        do_request = True
        cache_hit = False
        if self.cache_pages: # if cache is enable try to load last request
            file_name = self.cache_html_location+url.replace(self.domain,"").replace("/","").replace(":","")+".html"
            if os.path.isfile(file_name):
                logging.info("Cache HIT URL: {}".format(url))
                do_request = False
                cache_hit = True
                f = open(file_name, "r")
                content = f.read()
                f.close()

        if do_request: # load url from web
            logging.info("Requesting URL: {}".format(url))
            req = requests.get(url, headers=self.base_header)
            content = req.text

        if self.cache_pages and not cache_hit: # if cache is enable save result
            logging.info("Saving cache URL: {}".format(url))
            f = open(file_name, "w")
            f.write(content)
            f.close()

        res = BeautifulSoup(content, "html.parser")
        return res

    def get_html(self, url): # retry controll from get html
        limit_error = 0
        while limit_error<3:
            limit_error += 1
            try:
                return self.get_html_protected(url)
            except Exception as e:
                logging.error("ERROR getting HTML: {}".format(e))
                time.sleep(limit_error*30) # sleep for 0, 30, 60...


class Manager(Base):

    def __init__(self, base_url, max_request, cache_pages=False):
        logging.info("Creating manager")

        super().__init__(cache_pages)
        self.base_url = base_url
        if self.base_url[-1]=="/":
            self.base_url = self.base_url[0:-1] # remove "/" from the url to allow concatenation
        self.domain = urlparse(self.base_url).netloc
        self.config_folder = "./config/" # base config folder. The domain folder will be created inside
        self.categories_folder = None # Folder to save info about categories
        self.threads_folder = None
        self.config_file = None

        self.threads = []
        self.threads_href = {}


        self.max_request = max_request
        if self.max_request <1 or self.max_request > 100:
            raise Exception("Invalid max_request parameter")



        self.id_counter = 0
        #locks
        self.lock_new_id = threading.Lock()




        self.warm_up() # must be last, but before create_config
        self.create_configs() # depends on warm_up and locks

        logging.info("Manager created")

    def get_new_id(self):
        self.lock_new_id.acquire()
        v = self.id_counter
        self.id_counter += 1

        config_file = self.config_folder+"config.json" # define config file
        f = open(config_file, 'r')
        data = json.loads(f.read())
        f.close()
        data['last_id'] = self.id_counter

        self.write_json(config_file, data)

        self.lock_new_id.release()
        return v



    def create_configs(self):
        logging.info("Creating configs")


        if not os.path.exists(self.config_folder): # create config folder
            logging.info("Creating folder: {}".format(self.config_folder))
            os.makedirs(self.config_folder)

        self.config_folder = self.config_folder+"{}/".format(self.domain)
        logging.info("Config folder defined as: {}".format(self.config_folder))

        if not os.path.exists(self.config_folder): # create config folder
            logging.info("Creating folder: {}".format(self.config_folder))
            os.makedirs(self.config_folder)

        self.threads_folder = self.config_folder+"threads/"
        logging.info("Threads folder defined as: {}".format(self.threads_folder))
        if not os.path.exists(self.threads_folder): # create threads folder
            logging.info("Creating folder: {}".format(self.threads_folder))
            os.makedirs(self.threads_folder)

        self.config_file = self.config_folder+"config.json" # define config file
        logging.info("Config file defined as: {}".format(self.config_file))


        if os.path.isfile(self.config_file): # create config folder
            logging.info("Reading config file")
            f = open(self.config_file, "r")
            data = json.loads(f.read())
            f.close()

            self.id_counter = data['last_id']

        else:
            logging.info("Creating file defined as: {}".format(self.config_file))
            data = { # information to save into domain file
                'domain': self.domain,
                'url': self.base_url,
                'last_id': 0,
            }
            self.write_json(self.config_file, data)


        self.get_categories() # auto load das categorias


    def get_categories(self):
        logging.info("Getting categories")


        categories_file = self.config_folder+"categories.json"
        logging.info("Categories file defined as: {}".format(categories_file))


        self.categories_folder = self.config_folder+"categories_threads/"
        logging.info("Categories folder defined as: {}".format(self.categories_folder))
        if not os.path.exists(self.categories_folder): # create config folder
            logging.info("Creating categories folder: {}".format(self.categories_folder))
            os.makedirs(self.categories_folder)



        logging.info("Requesting categories from {}".format(self.base_url))

        html = self.get_html(self.base_url)
        div_categories = html.body.find("div", class_="p-body-pageContent").find_all("div", class_="block--category")

        categories = []
        for cat in div_categories:
            title = cat.find("h2", class_="block-header").a.text
            href = cat.find("h2", class_="block-header").a['href']
            href = urllib.parse.urljoin(self.base_url, href)

            logging.info("Categorie found: {} {}".format(title, href))
            subs = []
            for subcat in cat.find_all("div", class_="node"):
                subtitle = subcat.find('h3', class_="node-title").a.text
                subhref = subcat.find('h3', class_="node-title").a['href']
                subhref = urllib.parse.urljoin(self.base_url, subhref)
                subs.append({
                    'id': None,
                    'title_text': subtitle,
                    'title_href': subhref,
                    'description': subcat.find('div', class_="node-description").text,
                    'last_update': datetime.min,
                    'complete': False,
                })
                logging.info("Subcategorie found: {} {}".format(subtitle, subhref))

            categories.append({'id': None, 'title_text': title, 'title_href': href, 'subs': subs})

        self.categories = categories


        if os.path.isfile(categories_file): # check_cache
            logging.info("Reading cache from categories")

            f = open(categories_file, "r")
            dat = f.read()
            f.close()

            categories_stored = json.loads(dat)

            save_again_disk = False
            # check for match between website categories and disk categories

            for category_stored in categories_stored: # added tag to determine if a match was found
                category_stored['matched'] = False
                for sub_stored in category_stored['subs']:
                    sub_stored['matched'] = False

            for category in self.categories:
                for category_stored in categories_stored:
                    if (category_stored['title_text'] == category['title_text']) and (category_stored['title_href'] == category['title_href']):
                        category_stored['matched'] = True
                        category['id'] = category_stored['id']
                        logging.info("Category found matched disk record: {}".format(category['id']))

                        for sub in category['subs']:
                            for sub_stored in category_stored['subs']:
                                if (sub_stored['title_text'] == sub['title_text']) and (sub_stored['title_href'] == sub['title_href']):
                                    sub['id'] = sub_stored['id']
                                    logging.info("Subcategorie found matched disk record: {}".format(sub['id']))
                                    sub_stored['matched'] = True
                                    break

                            if sub['id']==None:
                                sub['id'] = self.get_new_id()
                                logging.info("SubCategory is new to disk record: {}".format(str(sub)))
                                save_again_disk = True

                if category['id']==None:
                    category['id'] = self.get_new_id()
                    save_again_disk = True
                    for sub in category['subs']:
                        sub['id'] = self.get_new_id()
                    logging.info("Category is new to disk record: {}".format(str(category)))

            ok = True
            for category_stored in categories_stored: # check for missing items and insert them. They must be inserted in order to not loose information about discontinued categories and subs
                if not category_stored['matched']:
                    logging.warning("Category not matched. This category will be inserted: {}".format(str(category_stored)))
                    ok = False
                    for sub_stored in category_stored['subs']:
                        sub_stored['matched'] = True
                self.categories.append(category_stored)
                category_stored['matched'] = True
                for sub_stored in category_stored['subs']:
                    if not sub_stored['matched']:
                        logging.warning("SUBCategory not matched. This SUBCategory will be inserted: {}".format(str(sub_stored)))
                        ok = False
                        sub_stored['matched'] = True
                        category_stored['subs'].append(sub_stored)

            if ok:
                logging.info("No missing categories and subcategories")
            else:
                save_again_disk = True


            if save_again_disk:
                self.write_json(categories_file, self.categories)


            # TODO: this piece of code only works on python 3.7+
            # for cat in self.categories:
            #     for subcat in cat['subs']:
            #         subcat['last_update'] = datetime.fromisoformat(subcat['last_update']) # transform iso to datetime type python 3.7+
            logging.info("Categories cache loaded completed")

        else: # actually request the page and reload categories file
            for category in self.categories:
                category['id'] = self.get_new_id()
                for sub in category['subs']:
                    sub['id'] = self.get_new_id()
            self.write_json(categories_file, self.categories)
            pass



        logging.info("Categorie loaded completed")

    def get_threads_page(self, cat_id, sub_id, url):
        logging.info("getthreadspage Starting: {} {} {}".format(cat_id, sub_id, url))
        initial_url = ""+url

        dst_file = self.categories_folder+"category_{}_subcategory_{}.json".format(cat_id, sub_id)
        logging.info("getthreadspage file: {}".format(dst_file))


        threads = []
        page = 0
        total_threads = 0
        url = initial_url



        res = {}
        if os.path.isfile(dst_file):
            logging.info("getthreadspage file exist!")
            with open(dst_file, "r") as f:
                res = json.loads(f.read())

            page = res['total_pages']
            total_threads = res['total_threads']
        else:
            logging.info("Creating category file: {}".format(dst_file))
            res = {
                'url': initial_url,
                'category': cat_id,
                'subcategory': sub_id,
                'threads': [],
                'status': 'started',
                'total_pages': 0,
                'total_threads': 0,
            }

            self.write_json(dst_file, res)


        logging.info("len(res['threads'])>0 {}".format(len(res['threads'])>0))
        if len(res['threads'])>0:
            time_stop_thread = None
            try:
                t = res['threads'][0]['last_post']
                logging.info("file already hast last post. last_post_time: {}".format(t))
                try:
                    t = datetime.fromisoformat(t)
                except:
                    t = t[0:-2]+":"+t[-2:]
                    t = datetime.fromisoformat(t)
                time_stop_thread = t
            except:
                logging.info("Using datetime.min as time_stop_thread")
                time_stop_thread = datetime.min
                time_stop_thread = time_stop_thread.replace(tzinfo=pytz.timezone('America/Sao_Paulo'))
        else:
            logging.info("No thread. Using datetime.min as time_stop_thread ")
            time_stop_thread = datetime.min
            time_stop_thread = time_stop_thread.replace(tzinfo=pytz.timezone('America/Sao_Paulo'))


        logging.info("Checking for neweset message in threads ")
        try:
            for i in range(len(res['threads'])):
                t = res['threads'][i]['last_post']
                t = t[0:-2]+":"+t[-2:]
                t = datetime.fromisoformat(t)
                if t>time_stop_thread:
                    time_stop_thread = t
        except KeyError as ke:
            logging.warning("File {} does not contemplate 'last_post' yet.".format(dst_file))
            pass


        reach_oldest_record = False


        while True:
            html = self.get_html(url)

            res['total_pages'] = int(html.find("ul", class_="pageNav-main").find_all('li')[-1].text)

            posts = html.find_all("div", class_="structItem-title")


            for post in posts:


                is_fixed = "structItemContainer-group--sticky" in post.parent.parent.parent['class']
                href = ""
                user_el = post.parent.find('div', class_='structItem-minor').li.a
                member_href = ""
                member_name = ""
                try:
                    member_href = user_el['href']
                    member_href = urllib.parse.urljoin(self.base_url, member_href)
                    member_name = user_el.text
                except:
                    pass
                date_thread = post.parent.parent.find('time')['datetime']

                tags_a = post.find_all('a')
                tags_thread = []

                title = tags_a[-1].text
                href = urllib.parse.urljoin(self.base_url, tags_a[-1]['href'])

                tags_thread = list(map(lambda x: x.text ,tags_a[0:-1]))

                answers = post.parent.parent.find('div', class_="structItem-cell--meta").find_all("dl")[0].find_all("dd")[0].text
                visits = post.parent.parent.find('div', class_="structItem-cell--meta").find_all("dl")[1].find_all("dd")[0].text

                try:
                    last_post = post.parent.parent.find("div", class_="structItem-cell--latest").find('time')['datetime']
                    last_post_datetime = last_post[0:-2]+":"+last_post[-2:]
                    last_post_datetime = datetime.fromisoformat(last_post_datetime)
                except:
                    last_post_datetime = datetime.min
                    last_post_datetime = last_post_datetime.replace(tzinfo=pytz.timezone('America/Sao_Paulo'))
                    last_post = default(last_post_datetime)
                    last_post = last_post[0:-3]+last_post[-2:]


                if not is_fixed:
                    if last_post_datetime<time_stop_thread: # Avoid to continue checking old threads
                        reach_oldest_record = True
                        break

                thread_found = {
                        'category': cat_id,
                        'subcategory': sub_id,
                        'title': title,
                        'href': href,
                        'member_href': member_href,
                        'member_name': member_name,
                        'date_thread': date_thread,
                        'tags': tags_thread,
                        'answers' : answers,
                        'visits' : visits,
                        'last_post' : last_post,
                        'is_fixed' : is_fixed,
                    }

                ok = False
                for i in range(len(res['threads'])):
                    if res['threads'][i]['href']==href:
                        ok = True
                        thread_found['id'] = res['threads'][i]['id']
                        res['threads'][i] = thread_found
                        break
                if not ok:
                    thread_found['id'] = self.get_new_id()

                    res['total_threads'] += 1 # TODO validate if this is a new thread
                    res['threads'].append(thread_found)


            self.write_json(dst_file, res)

            if reach_oldest_record: # Avoid to continue checking old threads
                break

            if len(html.find_all("a", class_="pageNav-jump--next"))>0:
                next_page_url = html.find("a", class_="pageNav-jump--next")['href']
                next_page_url = urllib.parse.urljoin(self.base_url, next_page_url)

                url = next_page_url
            else:
                break



        res['status'] = 'complete'
        self.write_json(dst_file, res)

        return True


    def print_summary(self):
        self.load_threads()
        logging.info('Showing summary')
        print("URL:\t\t\t{}".format(self.base_url))
        print("Domain:\t\t\t{}".format(self.domain))
        print("Amt. Categories:\t{}".format(len(self.categories)))
        print("Amt. SubCategories:\t{}".format(sum([len(x['subs']) for x in self.categories])))
        print("Amt. Threads:\t\t{}".format(len(self.threads)))


    def reload_threads(self):
        logging.info("Reloading threads")

        # Any updates on categories file must be in this function

        threads_running = []

        for category in self.categories:
            for sub in category['subs']:
                logging.info("Reloading threads from: {}".format(sub['title_href']))
                # self.get_threads_page(category['id'], sub['id'], sub['title_href'])

                x = threading.Thread(target=self.get_threads_page, args=(category['id'], sub['id'], sub['title_href'],))
                threads_running.append(x)
                x.start()

                while True:
                    threads_running = [thread for thread in threads_running if thread.is_alive()]
                    time.sleep(0.1)

                    if len(threads_running)<self.max_request:
                        break


        for x in threads_running:
            x.join()

        logging.info("Reloading threads completed")


    def load_threads(self):
        logging.info("Reading threads to memmory")

        self.threads = []
        self.threads_href = {}
        for file in os.listdir(self.categories_folder):
            dat = None
            with open(self.categories_folder+file, "r") as f:
                dat = f.read()

            dat = json.loads(dat)

            self.threads += dat['threads']

        for i in range(len(self.threads)):
            t = self.threads[i]
            self.threads_href[t['href']] = self.threads[i]

        logging.info("Reading threads to memmory completed. Total: {}".format(len(self.threads)))


    def write_json(self, dst, content):
        with open(dst, "w") as f:
            f.write(json.dumps(content, indent=2, default=default))

    def search_page_message(self, url, guess_page, limit):

        logging.info("Searching where to restart mining url: {} guesspage: {} limit: {}".format(url, guess_page, limit))
        found = False
        while guess_page>1:
            url_with_page = url+"page-{}".format(guess_page)
            logging.info("Checking url: {}".format(url_with_page))

            html = self.get_html(url_with_page)

            for message in html.find_all('article', class_="message--post"):

                creation_time = message.find('div', class_="message-cell--main").find('header').find('time')['datetime']
                creation_time = creation_time[0:-2]+":"+creation_time[-2:]
                creation_time = datetime.fromisoformat(creation_time)
                if creation_time<limit:
                    found = True
                    break

            if found:
                break
            guess_page -= 1

        logging.info("Page to restart is: {}".format(guess_page))
        return guess_page

    def requesta(self, thread):
        thread = thread.copy()
        try:
            logging.info("Requesting posts from {}".format(str(thread)))

            thread_file = self.threads_folder+"{}.json".format(thread['id'])

            page = 1
            url = None

            create_new_thread_file = True
            if os.path.isfile(thread_file): # TODO We are researching every thread. We may check the interval between the last two messages
                logging.info("Cache hit thread id {}".format(thread['id']))


                with open(thread_file, "r") as f:
                    thread = json.loads(f.read())

                if len(thread['messages'])>0:
                    create_new_thread_file = False

                    thread['status'] = "reloading"

                    most_recent_message = thread['messages'][-1]['creation']
                    most_recent_message = most_recent_message[0:-2]+":"+most_recent_message[-2:]
                    most_recent_message = datetime.fromisoformat(most_recent_message)

                    page = self.search_page_message(thread['href'], thread['total_pages'], most_recent_message)
                    ignore_before = most_recent_message

                    url = thread['href']
                    if page>1:
                        url = url+"page-{}".format(page)


            if create_new_thread_file:
                page = 1
                thread['status'] = "incomplete"
                thread['started'] = datetime.now()
                thread['total_pages'] = page # if repeate the variable here the json will look better
                thread['total_posts'] = 0
                thread['messages'] = []
                thread['last_update'] = datetime.now()
                url = thread['href']

                ignore_before = datetime.min.replace(tzinfo=pytz.timezone('America/Sao_Paulo'))
                self.write_json(thread_file, thread)

            try:
                while True:
                    html = self.get_html(url)

                    for message in html.find_all('article', class_="message--post"):


                        official_id = message['data-content']
                        user = message.find('div', class_="message-cell--user").find('a', class_="username")

                        try:
                            user_name = user.text
                            user_href = user['href']
                        except:
                            user_name = ''
                            user_href = ''

                        creation_time = message.find('div', class_="message-cell--main").find('header').find('time')['datetime']


                        creation_time_obj = creation_time[0:-2]+":"+creation_time[-2:]
                        creation_time_obj = datetime.fromisoformat(creation_time_obj)

                        if creation_time_obj<=ignore_before:
                            continue



                        message = str(message.find('div', class_="message-cell--main").find('article', class_='message-body').find('div', class_='bbWrapper'))


                        thread['messages'].append({
                            'official_id': official_id,
                            'user_name': user_name,
                            'user_href': user_href,
                            'creation': creation_time,
                            'message': message,
                        })


                    thread['total_pages'] = page
                    thread['total_posts'] = len(thread['messages'])
                    thread['last_update'] = datetime.now()

                    if len(html.find_all("a", class_="pageNav-jump--next"))>0:
                        url = html.find("a", class_="pageNav-jump--next")['href']
                        url = urllib.parse.urljoin(self.base_url, url)
                        page += 1
                    else:
                        thread['status'] = "complete"
                        break

            except Exception as e:
                thread['error'] = str(e)
                thread['status'] = "error"
                logging.error("ERROREXCEPTION (1) {} {} {}".format(str(thread['id']), str(e), url))


            self.write_json(thread_file, thread)
        except Exception as e:
            logging.error("ERROREXCEPTION (2) {} {}".format(str(thread), str(e)))



    def reload_posts(self):
        logging.info("Starting to reload posts")
        self.load_threads()

        threads_running = []
        for i in range(len(self.threads)):

            t = self.threads[i]

            x = threading.Thread(target=self.requesta, args=(t,))
            threads_running.append(x)
            x.start()


            while True:
                threads_running = [thread for thread in threads_running if thread.is_alive()]
                time.sleep(0.1)

                if len(threads_running)<self.max_request:
                    break

        for x in threads_running:
            x.join()

        logging.info("Reload posts completed")