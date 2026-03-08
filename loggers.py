from datetime import datetime, timedelta, timezone, UTC
from logging import Logger, Formatter, Handler, FileHandler, StreamHandler
import os, sys, time
from settings import BASE_PATH, LIMIT_LOG_WRITES_PER_HOUR



# Asia/Jakarta timezone i.e. GMT+7
wib_tz = timezone(timedelta(hours=7), name='Asia/Jakarta, WIB')

class DbHandler(Handler):
    def emit(self, record):
        pass
        # Log.create(level=record.levelname, file=record.filename + " - line " + str(record.lineno), message=record.exc_text or record.msg, created_at=record.asctime)

class LoggerFile(Logger):
    def __init__(self, name: str, level = 20, handlers=[]) -> None:
        super().__init__(name, level)
        for h in handlers: super().addHandler(h)
        self.counter = 0
        self.counter_date = datetime.now(tz=wib_tz)

    # Limit log writes
    def check_counter(func):
        def inner(self, msg, stacklevel=3, exc_info=True):
            if self.counter < LIMIT_LOG_WRITES_PER_HOUR :
                self.counter += 1
                return func(self, msg, stacklevel=stacklevel, exc_info=exc_info)
            elif self.counter_date + timedelta(hours=1) <= datetime.now(UTC):
                self.counter = 1
                self.counter_date = datetime.now(UTC)
                return func(self, msg, stacklevel=stacklevel, exc_info=exc_info)
        return inner

    @check_counter
    def error(self, msg, stacklevel=2, exc_info=True):
        super().error(msg=msg, exc_info=exc_info, stacklevel=stacklevel)
    
    @check_counter
    def critical(self, msg, stacklevel=2, exc_info=True):
        super().critical(msg=msg, exc_info=exc_info, stacklevel=stacklevel)


formatter = Formatter(fmt="%(asctime)s WIB %(levelname)s %(pathname)s:%(lineno)d - %(message)s", datefmt="%Y-%m-%d %H:%M:%S", defaults={'consumer': 'Prefect App'})
formatter.__setattr__("converter", lambda s: time.gmtime(s + 7*3600))       # Explicitly set to WIB (Asia/Jakarta)

handler_file = FileHandler(filename=f"{BASE_PATH}/logs.log", mode="a")
handler_file.setFormatter(formatter)
handler_file.setLevel(20)

handler_stdout = StreamHandler(sys.stdout)
handler_stdout.setFormatter(formatter)
handler_stdout.setLevel(10)

# handler_db = DbHandler(level=20)
# handler_db.setFormatter(formatter)

logger = LoggerFile(name="LoggerFile", level=10, handlers=[handler_file, handler_stdout])