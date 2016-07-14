#
# Plugin to collectd statistics from MongoDB
#

import collectd
from pymongo import ASCENDING
from pymongo import DESCENDING
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from distutils.version import StrictVersion as V

import math
import time
import re
import traceback

def tstofloat(d):
    return time.mktime(d.timetuple())

class MongoDBReplSet(object):

    def __init__(self):
        self.plugin_name = "mongodb_replset"
        self.mongo_host = "127.0.0.1"
        self.mongo_port = 27017
        self.mongo_user = None
        self.mongo_password = None

    def submit(self, replset, type, instance, value):
        self.submit_raw(self.plugin_name, replset, type, instance, value)

    def submit_raw(self, plugin_name, plugin_instance, type, instance, value):
        v = collectd.Values()
        v.plugin = plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()

    def do_status(self):
        con = MongoClient(host=self.mongo_host, port=self.mongo_port, read_preference=ReadPreference.SECONDARY)
        try:
            db = con['admin']
            if self.mongo_user and self.mongo_password:
                db.authenticate(self.mongo_user, self.mongo_password)

            self.do_replset_get_status(db)

            db = con['local']
            if self.mongo_user and self.mongo_password:
                db.authenticate(self.mongo_user, self.mongo_password)

            self.do_oplog_get_metrics(db)
        except:
            traceback.print_exc()
        finally:
            con.close()

    def do_oplog_get_metrics(self, db):
        self.do_get_replication_info_timestamps(db)
        self.do_get_replication_info_stats(db)

    def do_get_replication_info_timestamps(self, db):
        oplog_rs = db['oplog.rs']

        oplog_head = oplog_rs.find(sort=[('$natural',1)], limit=1)[0]['ts']
        oplog_tail = oplog_rs.find(sort=[('$natural',-1)], limit=1)[0]['ts']

        self.submit('', 'oplog', 'head_timestamp', oplog_head.time)
        self.submit('', 'oplog', 'tail_timestamp', oplog_tail.time)

        self.submit('', 'oplog', 'time_diff', oplog_tail.time - oplog_head.time)

    def do_get_replication_info_stats(self, db):

        oplog_info = db.command({ "collStats" : "oplog.rs" })

        count = oplog_info['count']
        self.submit('', 'oplog', 'items_total', count)

        size =  oplog_info['size']
        self.submit('', 'oplog', 'current_size_bytes', size)

        storageSize = oplog_info['storageSize']
        self.submit('', 'oplog', 'storage_size_bytes', storageSize)

        if 'maxSize' in oplog_info:
	    maxSize = oplog_info['maxSize']
	    logSizeMB = maxSize / (1024*1024)
            self.submit('', 'oplog', 'log_size_mb', logSizeMB)

	usedMB = size / (1024 * 1024)
        usedMB = math.ceil(usedMB * 100) / 100
        self.submit('', 'oplog', 'used_mb', usedMB)

    def do_replset_get_status(self, db):

        rs_status = db.command({"replSetGetStatus": 1})

        rs_name = rs_status['set']

        self.submit(rs_name, 'my_state', 'value', rs_status['myState'])

        if rs_status.has_key('term'):
            self.submit(rs_name, 'term', 'value', rs_status['term'])

        if rs_status.has_key('heartbeatIntervalMillis'):
            self.submit(rs_name, 'hearbeat_interval_ms', 'value', rs_status['heartbeatIntervalMillis'])

        primary_optime = None
        self_optime = None
        self_port = None

        self.submit(rs_name, 'member', 'count', len(rs_status['members']))

        t = 'member'
        for m in rs_status['members']:
            is_primary = m['stateStr'] == 'PRIMARY'
            is_self = m.get('self', False)

            host, port = m['name'].split(":")
            short_host = host.split(".")[0]
            if is_self:
                short_host = 'self'
                self_port = port

                n = "{0}-{1}".format(short_host, port)

            if not is_self and re.match('\d+\.\d+\.\d+\.\d+', host):
                n = "{0}-{1}".format(host,port)

            self.submit(rs_name, t, '{0}-uptime'.format(n), m['uptime'])
            self.submit(rs_name, t, '{0}-state'.format(n), m['state'])
            self.submit(rs_name, t, '{0}-health'.format(n), m['health'])

            if m.has_key('electionTime'):
                self.submit(rs_name, 'member','{0}.election_time'.format(n), m['electionTime'].time)

        if isinstance(m['optime'], dict):
            optime = m['optime']['ts'].time
        else:
            optime = m['optime'].time

        self.submit(rs_name, t, '{0}-optime_date'.format(n), optime)

        if is_primary:
            primary_optime = optime
        if is_self:
            self_optime = optime

        if m.has_key('lastHeartbeat'):
            self.submit(rs_name, t, '{0}-last_heartbeat'.format(n), tstofloat(m['lastHeartbeat']))

        if m.has_key('lastHeartbeatRecv'):
            self.submit(rs_name, t, '{0}-last_heartbeat_recv'.format(n), tstofloat(m['lastHeartbeatRecv']))
        if m.has_key('pingMs'):
            self.submit(rs_name, t, '{0}-ping_ms'.format(n), m['pingMs'])

        if self_optime != None and primary_optime != None:
            n = "self-{0}".format(self_port)
            self.submit(rs_name, t, '{0}-replication_lag'.format(n), primary_optime - self_optime)

    def config(self, obj):
        for node in obj.children:
            if node.key == 'Port':
                self.mongo_port = int(node.values[0])
            elif node.key == 'Host':
                self.mongo_host = node.values[0]
            elif node.key == 'User':
                self.mongo_user = node.values[0]
            elif node.key == 'Password':
                self.mongo_password = node.values[0]
            else:
                collectd.warning("mongodb_replset plugin: Unkown configuration key %s" % node.key)


mongodb_replset = MongoDBReplSet()
collectd.register_config(mongodb_replset.config)
collectd.register_read(mongodb_replset.do_status)
