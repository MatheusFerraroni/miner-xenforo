
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


    def get_html(self, url): # get html with cache
        logging.info("Will request URL: {}".format(url))

        file_name = None
        do_request = True
        if self.cache_pages: # if cache is enable try to load last request
            file_name = self.cache_html_location+url.replace(self.domain,"").replace("/","").replace(":","")+".html"
            if os.path.isfile(file_name):
                logging.info("Cache HIT URL: {}".format(url))
                do_request = False
                f = open(file_name, "r")
                content = f.read()
                f.close()

        if do_request: # load url from web
            logging.info("Requesting URL: {}".format(url))
            req = requests.get(url, headers=self.base_header)
            content = req.text

        if self.cache_pages: # if cache is enable save result
            logging.info("Saving cache URL: {}".format(url))
            f = open(file_name, "w")
            f.write(content)
            f.close()

        res = BeautifulSoup(content, "html.parser")
        return res

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
        self.load_threads()


    def get_categories(self):
        logging.info("Getting categories")

        categories_file = self.config_folder+"categories.json"
        logging.info("Categories file defined as: {}".format(categories_file))


        self.categories_folder = self.config_folder+"categories_threads/"
        logging.info("Categories folder defined as: {}".format(self.categories_folder))
        if not os.path.exists(self.categories_folder): # create config folder
            logging.info("Creating categories folder: {}".format(self.categories_folder))
            os.makedirs(self.categories_folder)


        if os.path.isfile(categories_file): # check_cache
            logging.info("Reading cache from categories")

            f = open(categories_file, "r")
            dat = f.read()
            f.close()

            self.categories = json.loads(dat)

            # for cat in self.categories:
            #     for subcat in cat['subs']:
            #         subcat['last_update'] = datetime.fromisoformat(subcat['last_update']) # transform iso to datetime type python 3.7+
            logging.info("Categories cache loaded completed")

        else: # actually request the page and reload categories file
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
                        'id': self.get_new_id(),
                        'title_text': subtitle,
                        'title_href': subhref,
                        'description': subcat.find('div', class_="node-description").text,
                        'last_update': datetime.min,
                        'complete': False,
                    })
                    logging.info("Subcategorie found: {} {}".format(subtitle, subhref))

                categories.append({'id': self.get_new_id(), 'title_text': title, 'title_href': href, 'subs': subs})

            self.categories = categories

            self.write_json(categories_file, self.categories)
        logging.info("Categorie loaded completed")



    def get_threads_page(self, cat_id, sub_id, url):
        # TODO: get only missing threads

        initial_url = ""+url
        url = initial_url

        res = []
        page = 0
        total_threads = 0
        while True:
            page += 1
            html = self.get_html(url)
            
            posts = html.find_all("div", class_="structItem-title")
            
            for post in posts:
                total_threads += 1
                title = []
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
                
                res.append({
                        'id': self.get_new_id(),
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
                    })


            if len(html.find_all("a", class_="pageNav-jump--next"))>0:
                next_page_url = html.find("a", class_="pageNav-jump--next")['href']
                next_page_url = urllib.parse.urljoin(self.base_url, next_page_url)

                url = next_page_url
            else:
                break



        res = {
            'url': initial_url,
            'category': cat_id,
            'subcategory': sub_id,
            'total_pages': page,
            'total_threads': total_threads,
            'threads': res,
        }

        dst_file = self.categories_folder+"category_{}_subcategory_{}.json".format(cat_id, sub_id)

        self.write_json(dst_file, res)

        return True
            
    def print_summary(self):
        logging.info('Showing summary')
        print("URL:\t\t\t{}".format(self.base_url))
        print("Domain:\t\t\t{}".format(self.domain))
        print("Amt. Categories:\t{}".format(len(self.categories)))
        print("Amt. SubCategories:\t{}".format(sum([len(x['subs']) for x in self.categories])))
        print("Amt. Threads:\t\t{}".format(len(self.threads)))


    def reload_threads(self):
        logging.info("Reloading threads")

        for category in self.categories:
            for sub in category['subs']:
                logging.info("Reloading threads from: {}".format(sub['title_href']))
                self.get_threads_page(category['id'], sub['id'], sub['title_href'])

        self.load_threads()

        logging.info("Reloading threads completed")

    def load_threads(self):
        logging.info("Reading threads to memmory")
        self.threads = []
        for file in os.listdir(self.categories_folder):
            dat = None
            with open(self.categories_folder+file, "r") as f:
                dat = f.read()

            dat = json.loads(dat)

            self.threads += dat['threads']
        logging.info("Reading threads to memmory completed")


    def write_json(self, dst, content):
        with open(dst, "w") as f:
            f.write(json.dumps(content, indent=2, default=default))

    def requesta(self, thread):
        try:
            logging.info("Requesting posts from {}".format(str(thread)))

            thread_file = self.threads_folder+"{}.json".format(thread['id'])

            if os.path.isfile(thread_file): # TODO: check time to update thread here. for now it just don't re-run
                logging.info("Cache hit thread id {}".format(thread['id']))
                return


            url = thread['href']
            page = 1

            thread['status'] = "incomplete"
            thread['started'] = datetime.now()
            thread['total_pages'] = page # if repeate the variable here the json will look better
            thread['total_posts'] = 0
            thread['last_update'] = datetime.now()


            thread['messages'] = []


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
                        break
    
                    self.write_json(thread_file, thread)
            except Exception as e:
                thread['error'] = str(e)
                logging.error("ERROREXCEPTION (1) {} {} {}".format(str(thread), str(e), url))
                pass


            thread['status'] = "complete"
            self.write_json(thread_file, thread)
        except Exception as e:
            logging.error("ERROREXCEPTION (2) {} {}".format(str(thread), str(e)))



    def reload_posts(self):
        logging.info("Starting to reload posts")

        threads_running = []
        for i in range(len(self.threads)):
            
            t = self.threads[i]

            x = threading.Thread(target=self.requesta, args=(t,))
            threads_running.append(x)
            x.start()

            if len(threads_running)>=self.max_request:
                for x in threads_running:
                    x.join()
                threads_running = []

        for x in threads_running:
            x.join()

        logging.info("Reload posts completed")