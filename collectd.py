import time

class ConfigNode:
  def __init__(self, key, values):
    self.key = key
    self.values = values

class Config:
  def __init__(self):
    self.children = []

  def add(self, key, value):
    self.children.append(ConfigNode(key,[value]))

class Values():
  def __init__(self):
    self.plugin = None
    self.plugin_instance = None
    self.type = None
    self.type_instance = None
    self.values = []

  def dispatch(self):
    plugin = "{}".format(self.plugin)

    if self.plugin_instance:
      plugin = "{}-{}".format(self.plugin, self.plugin_instance)

    for v in self.values:
      print "...{}.{}-{} {}".format(plugin, self.type, self.type_instance, v)

def warning(message):
  print "{}".format(message)

def register_read(plugin_query_func):
  plugin_query_func()
  print "..."

def register_config(plugin_config):
  test_config = Config()
  test_config.add('Host', '192.168.99.100')
  test_config.add('Port', '37268')

  plugin_config(test_config)
