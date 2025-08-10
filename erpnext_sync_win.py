import time
import os
import logging
from logging.handlers import RotatingFileHandler
from SMWinservice import SMWinservice
import local_config as config
from erpnext_sync import main  # Your cron-friendly merged script

class ERPNextBiometricPushService(SMWinservice):
    _svc_name_ = "ERPNextBiometricPushService"
    _svc_display_name_ = "ERPNext Biometric Push Service"
    _svc_description_ = "Service to push biometric data from BioTime to ERPNext"

    def start(self):
        self.isrunning = True
        self.setup_logging()

    def stop(self):
        self.isrunning = False

    def setup_logging(self):
        """Set up rotating log files exactly like the Linux version."""
        if not os.path.exists(config.LOGS_DIRECTORY):
            os.makedirs(config.LOGS_DIRECTORY)

        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

        log_file = os.path.join(config.LOGS_DIRECTORY, 'logs.log')
        err_file = os.path.join(config.LOGS_DIRECTORY, 'error.log')

        # Info logger
        info_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=50)
        info_handler.setFormatter(formatter)
        self.info_logger = logging.getLogger('info_logger')
        self.info_logger.setLevel(logging.INFO)
        if not self.info_logger.hasHandlers():
            self.info_logger.addHandler(info_handler)

        # Error logger
        err_handler = RotatingFileHandler(err_file, maxBytes=10_000_000, backupCount=50)
        err_handler.setFormatter(formatter)
        self.error_logger = logging.getLogger('error_logger')
        self.error_logger.setLevel(logging.ERROR)
        if not self.error_logger.hasHandlers():
            self.error_logger.addHandler(err_handler)

    def main(self):
        """Run ERPNext sync periodically."""
        while self.isrunning:
            try:
                self.info_logger.info("Starting ERPNext Biometric Push cycle...")
                main()  # Run the sync job
                self.info_logger.info("ERPNext Biometric Push cycle completed.")
            except Exception as e:
                self.error_logger.exception(f"Error during service run: {e}")
            time.sleep(config.PULL_FREQUENCY * 60)  # Use same frequency as config

if __name__ == '__main__':
    ERPNextBiometricPushService.parse_command_line()
