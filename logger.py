import logging
import os
from logging.handlers import TimedRotatingFileHandler

class AppLogger:
    """A logger that writes logs to a daily rotating log file (app-YYYYMMDD.log)."""

    def __init__(self, log_dir="logs"):
        """Initialize the logger with a daily rotating file handler."""
        os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists

        log_file = os.path.join(log_dir, "app.log")  # Base filename
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logging.INFO)

        # Create a rotating file handler (new file daily)
        handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
        handler.suffix = "%Y%m%d"  # Adds YYYYMMDD to rotated log files
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # Avoid duplicate handlers if this logger is re-initialized
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def log(self, message, level="INFO"):
        """Log a message at the specified level."""
        log_levels = {
            "INFO": self.logger.info,
            "WARNING": self.logger.warning,
            "ERROR": self.logger.error,
            "DEBUG": self.logger.debug,
            "CRITICAL": self.logger.critical
        }
        log_levels.get(level, self.logger.info)(message)  # Default to INFO

