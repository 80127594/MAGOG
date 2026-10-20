import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

from .config import Settings

logger = logging.getLogger(__name__)

class InMemoryHandler(logging.Handler):
    
    def __init__(self, capacity: int = 1000):
        super().__init__()
        self.capacity = capacity
        self.buffer: list[str] = []
    
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.buffer.append(msg)
            if len(self.buffer) > self.capacity:
                self.buffer.pop(0)
        except Exception:
            self.handleError(record)
    
    def get_logs(self, last_n: int | None = None) -> list[str]:
        if last_n is None:
            return self.buffer.copy()
        return self.buffer[-last_n:]
    
    def clear(self) -> None:
        self.buffer.clear()

_memory_handler: InMemoryHandler | None = None
def get_memory_handler() -> InMemoryHandler:
    if _memory_handler is None:
        raise RuntimeError("Logging not configured; call setup_logging() first")
    return _memory_handler


def setup_logging(settings: Settings) -> None:
    global _memory_handler
    
    log_cfg = settings.get("logging", {})
    log_level_str = log_cfg.get("level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    handlers: list[logging.Handler] = []
    
    # console
    if log_cfg.get("console", True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)
        logger.debug("Console logging enabled")
    
    # rotating file
    log_file = log_cfg.get("file")
    if log_file:
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        target = logs_dir / log_file
        
        # rotate existing log on startup
        if target.exists():
            ts = datetime.now().strftime("%Y.%m.%d.%H.%M.%S")
            rotated = logs_dir / f"{ts}-{log_file}"
            target.rename(rotated)
        
        max_bytes = log_cfg.get("file_max_bytes", 10 * 1024 * 1024)  # 10MB
        backup_count = log_cfg.get("file_backup_count", 5)
        
        file_handler = RotatingFileHandler(
            target,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        logger.debug(f"File logging enabled: {target} (max {max_bytes} bytes, {backup_count} backups)")
    
    buffer_capacity = log_cfg.get("buffer_capacity", 1000)
    _memory_handler = InMemoryHandler(capacity=buffer_capacity)
    _memory_handler.setFormatter(formatter)
    handlers.append(_memory_handler)
    logger.debug(f"In-memory log buffer enabled (capacity: {buffer_capacity})")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    for handler in handlers:
        root_logger.addHandler(handler)
    
    logger.info(f"Logging configured: level={log_level_str}, handlers={len(handlers)}")