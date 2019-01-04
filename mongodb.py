#
# Plugin to collectd statistics from MongoDB
#

import collectd
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from distutils.version import LooseVersion as V

import traceback
import re
import time
import math


def tstofloat(d):
    return time.mktime(d.timetuple())

class MongoDB(object):

    def __init__(self):
        self.plugin_name = "mongodb"
        self.mongo_host = "127.0.0.1"
        self.mongo_port = 27017
        self.mongo_db = ["admin", ]
        self.mongo_user = None
        self.mongo_password = None

        self.lockTotalTime = None
        self.lockTime = None
        self.accesses = None
        self.misses = None

    def submit(self, type, instance, value, db=None):
        if db:
            plugin_instance = '%s-%s' % (self.mongo_port, db)
        else:
            plugin_instance = str(self.mongo_port)
        v = collectd.Values()
        v.plugin = self.plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()

    def submit_repl_info(self, replset, type, instance, value):
        self.submit_raw(self.plugin_name, replset, type, instance, value)

    def submit_raw(self, plugin_name, plugin_instance, type, instance, value):
        v = collectd.Values()
        v.plugin = plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()

    def get_db_and_collection_stats(self):
        con = MongoClient(host=self.mongo_host, port=self.mongo_port, read_preference=ReadPreference.SECONDARY)
    	try:
            db = con['admin']
            if self.mongo_user and self.mongo_password:
                db.authenticate(self.mongo_user, self.mongo_password)
            self.do_simple_server_status(db)
#            self.do_server_status(db)
#            self.do_replset_get_status(db)

#            db = con['local']
#            if self.mongo_user and self.mongo_password:
#                db.authenticate(self.mongo_user, self.mongo_password)

#            self.do_oplog_get_metrics(db)
            
#            for mongo_db in self.mongo_db:
#                db = con[mongo_db]
#                if self.mongo_user and self.mongo_password:
#                    db.authenticate(self.mongo_user, self.mongo_password)
#                self.do_db_status(db, mongo_db)
        except:
            traceback.print_exc()
        finally:
            con.close()

    def do_simple_server_status(self, db):
        server_status = db.command('serverStatus')

        cnx_stat = server_status['connections']
        self.submit('cnx_count', 'current', cnx_stat['current'])
        #self.submit('cnx_count', 'available', cnx_stat['available'])
        self.submit('cnx_created_delta', 'created_per_second', cnx_stat['totalCreated'])
        
        net_stat = server_status['network']
        for t in ['bytesIn', 'bytesOut']: #, 'physicalBytesIn', 'physicalBytesOut']:
            self.submit('network', "{0}_per_sec".format(t), net_stat[t])

        # operations
        ops = server_status['opcounters']
        for t in ['getmore', 'query', 'insert', 'update', 'delete']:
            self.submit('opcounters', "{0}_per_sec".format(t), ops[t])




    def do_server_status(self, db):
        server_status = db.command('serverStatus')

        version = server_status['version']
        at_least_2_4 = V(version) >= V('2.4.0')

        self.submit('uptime','value', server_status['uptime'])

        # operations
        for k, v in server_status['opcounters'].items():
            self.submit('total_operations', k, v)
            self.submit('opcounters', k, v)
            
	    # repl operations
        for k, v in server_status['opcountersRepl'].items():
            self.submit('opcounters_repl', k, v)

        # memory
        for t in ['resident', 'virtual', 'mapped']:
            self.submit('memory', t, server_status['mem'][t])

        # asserts
        for k, v in server_status['asserts'].items():
            self.submit('asserts', k, v)

        # page faults
        self.submit('extra_info', 'page_faults', server_status['extra_info']['page_faults'])

        # connections
        self.submit('connections', 'current', server_status['connections']['current'])
        if 'available' in server_status['connections']:
            self.submit('connections', 'available', server_status['connections']['available'])
        if 'totalCreated' in server_status['connections']:
            self.submit('connections', 'totalCreated', server_status['connections']['totalCreated'])

        # metrics
        metrics = server_status['metrics']
        for k in ['document', 'operation', 'queryExecutor', 'record']:
                for i, val in metrics[k].items():
                    self.submit('metrics_{0}'.format(k.lower()), "{0}".format(i.lower()), val)

        # metrics/getlasterror
        self.submit('metrics_get_last_error','wtimeouts', server_status['metrics']['getLastError']['wtimeouts'])
        for k,v in server_status['metrics']['getLastError']['wtime'].items():
            self.submit('metrics_get_last_error', "wtime-{0}".format(k), v)

        # metrics/cursor
        self.submit('metrics_cursor','timed_out', server_status['metrics']['cursor']['timedOut'])
        for k,v in server_status['metrics']['cursor']['open'].items():
            self.submit('metrics_cursor', "open-{0}".format(k), v)

        # metrics/repl/executor metrics
        if 'executor' in metrics['repl']:
            for k, v in metrics['repl']['executor'].items():
                if k in ['networkInterface', 'shuttingDown']:
                    continue
                elif k in ['counters', 'queues']:
                    for a, b in v.items():
                        self.submit('metrics_repl_executor', "{0}-{1}".format(k, a), b)
                else:
                    self.submit('metrics_repl_executor', "{0}".format(k), v)

        # metrics/repl/apply metrics
        for k, v in metrics['repl']['apply'].items():
            if k in ['batches']:
                for a, b in v.items():
                    self.submit('metrics_repl_apply', "{0}-{1}".format(k, a), b)
            else:
                self.submit('metrics_repl_apply', "{0}".format(k), v)

        # metrics/repl/network metrics
        for k, v in metrics['repl']['network'].items():
            if k in ['getmores']:
                for a, b in v.items():
                    self.submit('metrics_repl_network', "{0}-{1}".format(k, a), b)
            else:
                self.submit('metrics_repl_network', "{0}".format(k), v)

        # metrics/repl/preload
        for k, v in metrics['repl']['preload'].items():
            if k in ['docs', 'indexes']:
                for a, b in v.items():
                    self.submit('metrics_repl_preload', "{0}-{1}".format(k, a), b)

        for k, v in metrics['repl']['buffer'].items():
            self.submit('metrics_repl_buffer', "{0}".format(k), v)


        # metrics/storage
        for k, v in metrics['storage'].items():
            for l, w in v.items():
                for m, x in w.items():
                    self.submit('metrics_storage_{0}'.format(k), "{0}-{1}".format(l,m), x)

        # metrics/ttl
        for k, v in metrics['ttl'].items():
            self.submit('metrics_ttl', "{0}".format(k), v)

        # network
        if 'network' in server_status:
            for t in ['bytesIn', 'bytesOut', 'numRequests']:
                self.submit('bytes', t, server_status['network'][t])

        # locks (pre 3.2)
        if 'lockTime' in server_status['globalLock']:
            if self.lockTotalTime is not None and self.lockTime is not None:
                if self.lockTime == server_status['globalLock']['lockTime']:
                    value = 0.0
                else:
                    value = float(server_status['globalLock']['lockTime'] - self.lockTime) * 100.0 / float(server_status['globalLock']['totalTime'] - self.lockTotalTime)
                self.submit('percent', 'lock_ratio', value)

            self.lockTime = server_status['globalLock']['lockTime']
            self.lockTotalTime = server_status['globalLock']['totalTime']
        else:
            self.submit('global_lock', 'total_time', server_status['globalLock']['totalTime'])
            for k in ['currentQueue','activeClients']:
                for m, v in server_status['globalLock'][k].items():
                    self.submit('global_lock', '{0}-{1}'.format(k.lower(), m), v)

        if 'locks' in server_status:
            for t, stats in server_status['locks'].items():
                typ = 'locks_{0}'.format(t.lower())
                if t == '.':
                  typ  = 'locks'
                for k, grouping in stats.items():
                    for s, v in grouping.items():
                        if s == 'r':
                            slabel = 'intent-shared-read'
                        elif s == 'w':
                            slabel = 'intent-excl-write'
                        elif s == 'R':
                            slabel = 'shared-read'
                        elif s == 'W':
                            slabel = 'excl-write'

                        self.submit(typ, '{0}-{1}'.format(k.lower(), slabel), v)


        # indexes
        if 'indexCounters' in server_status:
            accesses = None
            misses = None
            index_counters = server_status['indexCounters'] if at_least_2_4 else server_status['indexCounters']['btree']

            if self.accesses is not None:
                accesses = index_counters['accesses'] - self.accesses
                if accesses < 0:
                    accesses = None
            misses = (index_counters['misses'] or 0) - (self.misses or 0)
            if misses < 0:
                misses = None
            if accesses and misses is not None:
                self.submit('cache_ratio', 'cache_misses', int(misses * 100 / float(accesses)))
            else:
                self.submit('cache_ratio', 'cache_misses', 0)
            self.accesses = index_counters['accesses']
            self.misses = index_counters['misses']

    def do_db_status(self, db, mongo_db):
        db_stats = db.command('dbstats')

        # stats counts
        self.submit('counter', 'object_count', db_stats['objects'], mongo_db)
        self.submit('counter', 'collections', db_stats['collections'], mongo_db)
        self.submit('counter', 'num_extents', db_stats['numExtents'], mongo_db)
        self.submit('counter', 'indexes', db_stats['indexes'], mongo_db)

        # collection stats
        collections = db.collection_names()
        for collection in collections:
            collection_stats = db.command('collstats', collection)
            if 'wiredTiger' in collection_stats:
                if 'cursor' in collection_stats['wiredTiger']:
                    for k, v in collection_stats['wiredTiger']['cursor'].items():
                        self.submit('collection_stats', (collection + '-' + k), v, mongo_db)

        # stats sizes
        self.submit('file_size', 'storage', db_stats['storageSize'], mongo_db)
        self.submit('file_size', 'index', db_stats['indexSize'], mongo_db)
        self.submit('file_size', 'data', db_stats['dataSize'], mongo_db)

    def do_oplog_get_metrics(self, db):
        self.do_get_replication_info_timestamps(db)
        self.do_get_replication_info_stats(db)

    def do_get_replication_info_timestamps(self, db):
        oplog_rs = db['oplog.rs']

        oplog_head = oplog_rs.find(sort=[('$natural',1)], limit=1)[0]['ts']
        oplog_tail = oplog_rs.find(sort=[('$natural',-1)], limit=1)[0]['ts']

        self.submit_repl_info('', 'oplog', 'head_timestamp', oplog_head.time)
        self.submit_repl_info('', 'oplog', 'tail_timestamp', oplog_tail.time)

        self.submit_repl_info('', 'oplog', 'time_diff', int(oplog_tail.time - oplog_head.time))

    def do_get_replication_info_stats(self, db):

        oplog_info = db.command({ "collStats" : "oplog.rs" })

        count = oplog_info['count']
        self.submit_repl_info('', 'oplog', 'items_total', count)

        size =  oplog_info['size']
        self.submit_repl_info('', 'oplog', 'current_size_bytes', size)

        storageSize = oplog_info['storageSize']
        self.submit_repl_info('', 'oplog', 'storage_size_bytes', storageSize)

        if 'maxSize' in oplog_info:
            maxSize = oplog_info['maxSize']
            logSizeMB = maxSize / (1024*1024)
            self.submit_repl_info('', 'oplog', 'log_size_mb', logSizeMB)

        usedMB = size / (1024 * 1024)
        usedMB = math.ceil(usedMB * 100) / 100
        self.submit_repl_info('', 'oplog', 'used_mb', usedMB)

    def do_replset_get_status(self, db):

        rs_status = db.command({"replSetGetStatus": 1})

        rs_name = rs_status['set']

        self.submit_repl_info(rs_name, 'my_state', 'value', rs_status['myState'])

        if rs_status.has_key('term'):
            self.submit_repl_info(rs_name, 'term', 'value', rs_status['term'])

        if rs_status.has_key('heartbeatIntervalMillis'):
            self.submit_repl_info(rs_name, 'hearbeat_interval_ms', 'value', rs_status['heartbeatIntervalMillis'])

        primary_optime = None
        self_optime = None
        self_port = None

        self.submit_repl_info(rs_name, 'member', 'count', len(rs_status['members']))

        t = 'member'
        for m in rs_status['members']:
            is_primary = m['stateStr'] == 'PRIMARY'
            is_self = m.has_key('self')

            host, port = m['name'].split(":")
            short_host = host.split(".")[0]
            if is_self:
                short_host = 'self'
                self_port = port

            n = "{0}-{1}".format(short_host, port)

            if (not is_self) and re.match('\d+\.\d+\.\d+\.\d+', host):
                n = "{0}-{1}".format(host,port)

            self.submit_repl_info(rs_name, t, '{0}-uptime'.format(n), m['uptime'])
            self.submit_repl_info(rs_name, t, '{0}-state'.format(n), m['state'])
            self.submit_repl_info(rs_name, t, '{0}-health'.format(n), m['health'])

            if m.has_key('electionTime'):
                self.submit_repl_info(rs_name, 'member','{0}.election_time'.format(n), m['electionTime'].time)

            if 'optime' in m:
                if isinstance(m['optime'], dict):
                    optime = m['optime']['ts'].time
                else:
                    optime = m['optime'].time

                self.submit_repl_info(rs_name, t, '{0}-optime_date'.format(n), optime)

                if is_primary:
                    primary_optime = optime
                if is_self:
                    self_optime = optime

            if m.has_key('lastHeartbeat'):
                self.submit_repl_info(rs_name, t, '{0}-last_heartbeat'.format(n), tstofloat(m['lastHeartbeat']))

            if m.has_key('lastHeartbeatRecv'):
                self.submit_repl_info(rs_name, t, '{0}-last_heartbeat_recv'.format(n), tstofloat(m['lastHeartbeatRecv']))
            if m.has_key('pingMs'):
                self.submit_repl_info(rs_name, t, '{0}-ping_ms'.format(n), m['pingMs'])

        if self_optime != None and primary_optime != None:
            n = "self-{0}".format(self_port)
            self.submit_repl_info(rs_name, t, '{0}-replication_lag'.format(n), int(primary_optime - self_optime))

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
            elif node.key == 'Database':
                self.mongo_db = node.values
            else:
                collectd.warning("mongodb plugin: Unkown configuration key %s" % node.key)

mongodb = MongoDB()
collectd.register_read(mongodb.get_db_and_collection_stats)
collectd.register_config(mongodb.config)


