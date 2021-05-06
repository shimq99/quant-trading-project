#encoding:UTF-8
import logging
import logging.handlers
import datetime
import time

def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance
    
@singleton
class LogManager(object):

    def __init__(self,logName = 'defaultName'):
        self.logName = logName
        self.initLogger()

    
    def initLogger(self):
        fileName = 'log_' + self.logName+'_' + datetime.datetime.now().strftime('%Y%m%d') + '.log'
        
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filemode='a')

        '''
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        '''

        self.fileshandle = logging.handlers.TimedRotatingFileHandler(fileName, when='D', interval=1, backupCount=0)
        fmt_str = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
        self.fileshandle.suffix = "%Y%m%d.log"
        self.fileshandle.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt_str)
        self.fileshandle.setFormatter(formatter)
        logging.getLogger('').addHandler(self.fileshandle)
    
    def doRollover(self):
        logging.handlers.TimedRotatingFileHandler.doRollover(self.fileshandle)


if __name__ == '__main__':
        log = LogManager()
        time.sleep(1)
        logging.info('test1')
        time.sleep(1)
        log.doRollover()
        logging.info('test2')

