import argparse
import json
import os
from urllib.parse import urlparse

class Anonymizer:

    def __init__(self, url):
        self.base_url = url
        self.domain = urlparse(self.base_url).netloc

        self.config_folder     = "./config/{}/".format(self.domain) # must exist
        self.folder      = self.config_folder+"threads/" # must exist

        self.config = {'users_ids':{}, 'last_id': 0}

        if not os.path.exists(self.config_folder):
            raise Exception("Config folder not found: {}".format(self.config_folder))

        if not os.path.exists(self.folder): # create config folder
            raise Exception("Config folder not found: {}".format(self.folder))


    def anonymizer_user(self, uid):
        try:
            return self.config['users_ids'][uid]
        except:
            self.config['last_id'] += 1
            self.config['users_ids'][uid] = str(self.config['last_id'])
        return self.config['users_ids'][uid]

    def start(self):
        files = os.listdir(self.folder)

        for file in files:
            with open(self.folder+file, 'r') as file_o:
                content = file_o.read()

            content = json.loads(content)

            content['member_name'] = self.anonymizer_user(content['member_href'])
            content['member_href'] = ''

            for i in range(len(content['messages'])):
                content['messages'][i]['user_name'] = self.anonymizer_user(content['messages'][i]['user_href'])
                content['messages'][i]['user_href'] = ''

            with open(self.folder+file, 'w') as file_o:
                file_o.write(json.dumps(content, indent=2))


def main(url):
    anonymizer = Anonymizer(url)
    anonymizer.start()


if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument("-url", required=True, type=str, help="The base URL of the forum using XenForo")

    args = vars(ap.parse_args())

    main(args['url'])