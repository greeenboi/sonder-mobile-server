from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn_str = 'postgresql://neondb_owner:npg_4odJOPMmZx5q@ep-proud-violet-a1by6sib-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'

engine = create_engine(
    conn_str,
    pool_size=5,               # Maximum number of permanent connections
    max_overflow=10,           # Maximum number of additional temporary connections
    pool_timeout=30,           # Seconds to wait before timing out on getting a connection
    pool_recycle=1800,         # Recycle connections after 30 minutes
    pool_pre_ping=True,        # Test connections with a ping before using
    connect_args={
        "connect_timeout": 10, # Connection timeout in seconds
        "application_name": "my_app"  # Helps identify your app in Neon's logs
    }
)
SessionLocal = sessionmaker(autocommit = False, autoflush=False, bind=engine)

# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

def get_db(max_retries=3, retry_delay=1):
    retries = 0
    last_error = None

    while retries < max_retries:
        db = SessionLocal()
        try:
            # Test the connection with a simple query
            db.execute("SELECT 'hello neon';")
            yield db
            return
        except OperationalError as e:
            last_error = e
            db.close()
            retries += 1
            if retries >= max_retries:
                logger.error(f"Failed to connect to database after {max_retries} attempts: {e}")
                raise
            logger.warning(f"Database connection failed, retrying ({retries}/{max_retries})...")
            time.sleep(retry_delay * retries)  # Exponential backoff
        finally:
            db.close()

    # If we get here, all retries failed
    raise last_error