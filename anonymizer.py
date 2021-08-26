import argparse
import os
from urllib.parse import urlparse

class Anonymizer:

    def __init__(self, url):
        self.base_url = url
        self.domain = urlparse(self.base_url).netloc

        self.config_folder     = "./config/{}/".format(self.domain) # must exist
        self.clear_folder      = self.config_folder+"clear_threads/" # must exist
        self.result_folder     = self.config_folder+"clear_threads_anonymous/" # must be created

        self.config = {'users_ids':{}, 'last_id': 0}

        if not os.path.exists(self.config_folder):
            raise Exception("Config folder not found: {}".format(self.config_folder))

        if not os.path.exists(self.clear_folder): # create config folder
            raise Exception("Config folder not found: {}".format(self.clear_folder))

        if not os.path.exists(self.result_folder): # create config folder
            print("Creating folder: {}".format(self.result_folder))
            os.makedirs(self.result_folder)

    def anonymizer_user(self, uid):
        try:
            return self.config['users_ids'][uid]
        except:
            self.config['last_id'] += 1
            self.config['users_ids'][uid] = str(self.config['last_id'])
        return self.config['users_ids'][uid]

    def start(self):
        
        files = os.listdir(self.clear_folder)

        for file in files:
            with open(self.clear_folder+file, 'r') as file_o:
                content = file_o.read()
            
            content = content.split("\n")
            content = [c.split("\t") for c in content]
            content = [[c[0], self.anonymizer_user(c[1]) , c[2]] for c in content]

            content = ["\t".join(c) for c in content]
            content = "\n".join(content)

            with open(self.result_folder+file, 'w') as file_o:
                file_o.write(content)

def main(url):

    anonymizer = Anonymizer(url)
    anonymizer.start()


if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument("-url", required=True, type=str, help="The base URL of the forum using XenForo")

    args = vars(ap.parse_args())

    main(args['url'])