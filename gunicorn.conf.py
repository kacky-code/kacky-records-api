import yaml

with open("config.yaml", "r") as c:
    conffile = yaml.load(c, yaml.FullLoader)

workers = conffile["workers"]
threads = conffile["threads"]
bind = f"{conffile['bind_hosts']}:{conffile['port']}"
wsgi_app = "kacky_records_api.app:app"
preload_app = True
