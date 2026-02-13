import logging
from logging.handlers import RotatingFileHandler

class AppLogger:

    def __init__(self):
         
        self.logger = self.create_logger(name='rdas-logger', log_file='log-rdas.log', level=logging.INFO)


    # This is the default logger for the app, returns the logger instance.
    def get_logger(self): 
        return self.logger


    def get_another_logger(self, name='url-logger', log_file='log-url.log',level=logging.INFO):        
        return self.create_logger(name, log_file)


    def create_logger(self, name, log_file, level=logging.INFO, max_bytes=1024*1024*10, backup_count=10):
        """
        Initializes the logger with RotatingFileHandler and custom formatting.

        :param name: Logger name (default: 'rdas-logger')
        :param log_file: File where logs will be written (default: 'rdas.log')
        :param level: Log level (default: logging.INFO)
        :param max_bytes: Maximum file size for log rotation (default: 10MB)
        :param backup_count: Number of backup log files to keep (default: 10)
        """
        # 0.
        logger = logging.getLogger(name)       

        # Avoid adding handlers multiple times
        if not logger.hasHandlers():
            # 1.
            logger.setLevel(level)
            
            # 2. Create rotating file handler
            file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)

            console_handler = logging.StreamHandler()
            
            # 2.1 Create formatter
            formatter = logging.Formatter(
                #'%(asctime)s %(levelname)s: %(message)s'
                '%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s'
                #'%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
            )

            # 2.2 Set formatter
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            #file_handler.setLevel(level)

            # 3. Add handler to the logger
            logger.addHandler(file_handler)   
            logger.addHandler(console_handler)       
                 
        
        return logger