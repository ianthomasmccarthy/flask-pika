import json
import pika
import datetime
import requests
import traceback
from Queue import Queue

__all__ = ['Pika']


class Fpika(object):

    def __init__(self, app=None, Logger=None):
        """Create the Flask Pika extension."""
        self.app = app
        if app is not None:
            self.init_app(app)

        if Logger:
            self.logger = Logger

    def init_app(self, app):
        """Initialize the Flask Pika extension"""
        pika_params                = app.config['FLASK_PIKA_PARAMS'].copy()
        self.orig_params           = app.config['FLASK_PIKA_PARAMS'].copy()
        pool_params                = app.config['FLASK_PIKA_POOL_PARAMS'].copy()
        self.debug                 = app.debug
        self.logger = app.logger
        self.pool_size             = 1
        self.pool_recycle          = -1
        self.pool_queue            = Queue()
        self.channel_recycle_times = {}
        self.channel_broken_times  = []

        if app.config.get('FLASK_PIKA_FAILOVER_PARAMS', None) is None:
            self.failover = False
        else:
            self.failover = True
            self.orig_fail_over_params = app.config['FLASK_PIKA_FAILOVER_PARAMS'].copy()
            pika_failover_params       = app.config['FLASK_PIKA_FAILOVER_PARAMS'].copy()
            self.tolerance             = app.config['FLASK_PIKA_TOLERANCE'][0]
            self.tolerance_interval    = app.config['FLASK_PIKA_TOLERANCE'][1]
            self.failback              = app.config['FLASK_PIKA_FAILBACK']
            self.failed_over           = False
            self.count_back            = 0
            if 'credentials' not in pika_failover_params:
                pika_failover_params = self.create_creds(pika_failover_params)
                self._pika_failover_connection_params = pika.ConnectionParameters(**pika_failover_params)
                self.__DEBUG("Fail Over Connection params are %s" % self._pika_failover_connection_params)

        # fix create credentials if needed
        if 'credentials' not in pika_params:
            pika_params          = self.create_creds(pika_params)
            self._pika_connection_params = pika.ConnectionParameters(**pika_params)
            self.__DEBUG("Connection params are %s" % self._pika_connection_params)

        # setup pooling if requested
        if pool_params is not None:
            self.pool_size = pool_params['pool_size']
            self.pool_recycle = pool_params['pool_recycle']
            for i in xrange(self.pool_size):
                channel = PrePopulationChannel()
                self.__set_recycle_for_channel(channel, -1)
                self.pool_queue.put(channel)
            self.__DEBUG("Pool params are %s" % pool_params)

    def create_creds(self, params):
        """ Create Credentials and remove username and password fields. """
        usr = params['username']
        pas = params['password']
        del params['username']
        del params['password']
        params['credentials'] = pika.PlainCredentials(usr, pas)
        return params

    def __create_channel(self):
        """Create a connection and a channel based on pika params"""
        pika_connection = pika.BlockingConnection(self._pika_connection_params)
        channel = pika_connection.channel()
        self.__DEBUG("Created AMQP Connection and Channel %s" % channel)
        self.__set_recycle_for_channel(channel)
        return channel

    def __destroy_channel(self, channel):
        """Destroy a channel by closing it's underlying connection"""
        self.__remove_recycle_time_for_channel(channel)
        try:
            channel.connection.close()
            self.__DEBUG("Destroyed AMQP Connection and Channel %s" % channel)
        except Exception, e:
            self.__WARN("Failed to destroy channel cleanly %s" % e)

    def __set_recycle_for_channel(self, channel, recycle_time = None):
        """Set the next recycle time for a channel"""
        if recycle_time is None:
            recycle_time = (unix_time_millis_now() + (self.pool_recycle * 1000))

        self.channel_recycle_times[hash(channel)] = recycle_time

    def __remove_recycle_time_for_channel(self, channel):
        """Remove the recycle time for a given channel if it exists"""
        channel_hash = hash(channel)
        if channel_hash in self.channel_recycle_times:
            del self.channel_recycle_times[channel_hash]

    def __should_recycle_channel(self, channel):
        """Determine if a channel should be recycled based on it's recycle time"""
        recycle_time = self.channel_recycle_times[hash(channel)]
        return recycle_time < unix_time_millis_now()

    def channel(self):
        """
            Get a channel
            If pooling is setup, this will block until a channel is available
            If pooling is not setup, a new channel will be created
        """
        # if using pooling
        if self.pool_recycle > -1:
            # get channel from pool or block until channel is available
            ch = self.pool_queue.get()
            self.__DEBUG("Got Pika channel from pool %s" % ch)

            # recycle channel if needed or extend recycle time
            if self.__should_recycle_channel(ch):
                old_channel = ch
                self.__destroy_channel(ch)
                ch = self.__create_channel()
                self.__DEBUG("Pika channel is too old, recycling channel %s and replacing it with %s"  % (old_channel, ch))
            else:
                self.__set_recycle_for_channel(ch)

            # make sure our channel is still open
            while ch is None or not ch.is_open:
                old_channel = ch
                self.__destroy_channel(ch)
                ch = self.__create_channel()
                self.__WARN("Pika channel not open, replacing channel %s with %s" % (old_channel, ch))

        # if not using pooling
        else:
            # create a new channel
            ch = self.__create_channel()

        return ch

    def return_channel(self, channel):
        """
            Return a channel
            If pooling is setup, will return the channel to the channel pool
                **unless** the channel is closed, then channel is passed to return_broken_channel
            If pooling is not setup, will destroy the channel
        """
        # if using pooling
        if self.pool_recycle > -1:
            self.__DEBUG("Returning Pika channel to pool %s" % channel)
            if self.failover:
                if self.failed_over:
                    self.count_back += 1
                    self.__WARN(self.count_back)
                if self.count_back >= self.failback:
                    self.__WARN("checking rabbit count is {c} >= {t}".format(c=self.count_back, t=self.failback))
                    self.check_rabbit()
            if channel.is_open:
                self.pool_queue.put(channel)
            else:
                self.return_broken_channel(channel)

        # if not using pooling then just destroy the channel
        else:
            if self.failover:
                if self.failed_over:
                    self.count_back += 1
                    self.__WARN(self.count_back)
                if self.count_back >= self.failback:
                    self.__WARN("checking rabbit count is {c} >= {t}".format(c=self.count_back, t=self.failback))
                    self.check_rabbit()
            self.__destroy_channel(channel)

    def return_broken_channel(self, channel):
        """
            Return a broken channel
            If pooling is setup, will destroy the broken channel and replace it in the channel pool with a new channel
            If pooling is not setup, will destroy the channel
        """
        # if using pooling
        if self.pool_recycle > -1:
            self.__WARN("Pika channel returned in broken state, replacing %s" % channel)
            self.channel_broken_times.append(unix_time_millis_now()/1000)
            self.__destroy_channel(channel)
            if self.failover:
                if not self.failed_over:
                    self.__DEBUG("FlaskPika: Checking failing Tolerance")
                    if self.check_tolerance():
                        self._fail_over("failover")
                else:
                    self.__DEBUG("return_broken_channel(): Already Failed Over.")
                    self.count_back += 1
                    self.__WARN(self.count_back)
                    if self.count_back >= self.failback:
                        self.check_rabbit()
                        self.__WARN("checking rabbit count is {c} >= {t}".format(c=self.count_back, t=self.failback))

            self.pool_queue.put(self.__create_channel())

        # if not using pooling then just destroy the channel
        else:
            self.__WARN("Pika channel returned in broken state %s" % channel)
            self.__destroy_channel(channel)
            if self.failover:
                if self.check_tolerance():
                    self._fail_over("failover")
                if self.failed_over:
                    self.count_back += 1
                    self.__WARN(self.count_back)
                if self.count_back >= self.failback:
                    self.__WARN("checking rabbit count is {c} >= {t}".format(c=self.count_back, t=self.failback))
                    self.check_rabbit()

    def check_tolerance(self):
        self.__WARN("Failed Over status: {f}".format(f=str(self.failed_over)))
        if self.failed_over:
            return False
        self.__WARN("checking Tolerance.")
        self.__WARN("")

        # self.channel_broken_times = sorted(self.channel_broken_times, reverse=True)
        if len(self.channel_broken_times) > self.tolerance:
            del self.channel_broken_times[-(self.tolerance):]
        self.__DEBUG("FlaskPika: check_tolerance(): {cbt}".format(cbt=str(self.channel_broken_times)))
        evaluation = (unix_time_millis_now() / 1000) - self.tolerance_interval
        self.__DEBUG("FlaskPika: check_tolerance(): evaluation is {e:.2f}".format(e=evaluation))
        check = 0
        for item in self.channel_broken_times:
            self.__DEBUG("if {item:.2f} > {e:.2f}".format(item=item, e=evaluation))
            if item > evaluation:
                self.__DEBUG("CHECK")
                check += 1
        if check >= self.tolerance:
            return True
        else:
            return False

    def _fail_over(self, reason):
        try:
            if reason == "failover":
                self.__WARN("Pika Failed over to backup host.")
            else:
                self.__WARN("Pika Failed over to backup host, due to {r}.".format(r=reason))

            self.__DEBUG("Pika Connection Current is {cur}".format(cur=str(self._pika_connection_params)))
            tmp = self._pika_connection_params
            self._pika_connection_params = self._pika_failover_connection_params
            self._pika_failover_connection_params = tmp
            self.__DEBUG("Pika Connection Failed Over to is {cur}".format(cur=str(self._pika_connection_params)))
            if self.failed_over:
                self.failed_over = False
            else:
                self.failed_over = True
            return "Successfully Failed Over"
        except Exception as e:
            tb = traceback.format_exc()
            self.__WARN("Pika Failover Failed. Error: {e}, tb: {tb}".format(e=e,tb=tb))
            return "Failed to Fail Over, error: {e}".format(e=e)

    def check_rabbit(self):
        try:
            self.count_back = 0
            api_url = "http://{host}:15672/api".format(host=self.orig_params['host'])
            if self.check_alive(api_url):
                self._fail_over("Fail-Back")
                self.__WARN("check_rabbit(): Failing back to original rabbit instance.")
                self.failed_over = False
        except Exception as e:
            self.__WARN("check_rabbit(): error: {e}".format(e=e))

    def check_alive(self, api_url):
        url = api_url + "/aliveness-test/%2F"
        self.__DEBUG("printing orig params")
        self.__DEBUG(self.orig_params)
        try:
            auth = (self.orig_params['username'], self.orig_params['password'])
            tmp = requests.get(url, auth=auth)
            tmp = json.loads(tmp.text)
            self.__DEBUG("aliveness check: " + str(tmp))
            if "ok" in tmp['status']:
                return True
            else:
                return False
        except Exception as e:
            tb = traceback.format_exc()
            self.__WARN("check_alive(): error: {e}; tb: {tb}".format(e=e, tb=tb))

    def __DEBUG(self, msg):
        """Log a message at debug level if app in debug mode"""
        if self.debug:
            pre = "Flask_Pika: "
            self.logger.debug(str(pre) + str(msg))

    def __WARN(self, msg):
        """Log a message at warning level"""
        pre = "Flask_Pika: "
        self.logger.warn(str(pre) + str(msg))


class PrePopulationChannel(object):

    def __init__(self):
        self._connection = PrePopulationConnection()

    @property
    def connection(self):
        return self._connection


class PrePopulationConnection(object):

    def __init__(self):
        pass

    def close(self):
        pass


def unix_time(dt):
    """Return unix time in microseconds"""
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return int((delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 10**6) / 10**6)


def unix_time_millis(dt):
    """Return unix time in milliseconds"""
    return round(unix_time(dt) * 1000.0)


def unix_time_millis_now():
    """Return current unix time in milliseconds"""
    return unix_time_millis(datetime.datetime.utcnow())
