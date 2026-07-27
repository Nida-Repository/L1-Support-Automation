import sys
from sqlalchemy import text
from app.database import engine, SessionLocal, Base
# Import models to ensure they register properly with the Base metadata
from app.models import Site, Isp, Sensor, AlertState

def verify_setup():
    print("Starting Database and Model Verification...")
    
    # Test 1: Connection test
    try:
        print("Connecting to the database...")
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"Connection successful!")
            print(f"PostgreSQL Version: {version}\n")
    except Exception as e:
        print(f"Connection failed! Check your .env credentials and ensure PostgreSQL is running.")
        print(f"Error Details: {e}")
        sys.exit(1)

    # Test 2: Reflect/Create Table Schemas
    try:
        print("🛠️  Checking table schemas against database...")
        # This will create tables if they don't exist, or just safely bind if they do
        Base.metadata.create_all(bind=engine)
        print(" Models and table definitions successfully synchronized!\n")
    except Exception as e:
        print(" Schema mapping failed! There is a mismatch between models.py and your DB.")
        print(f"Error Details: {e}")
        sys.exit(1)

    # Test 3: Session Operations Test
    try:
        print("Testing basic query operations via ORM...")
        session = SessionLocal()
        
        # Count the alert states just to make sure we can query a table
        states_count = session.query(AlertState).count()
        print(f"ORM test successful! Connected to 'alert_states' table. Current row count: {states_count}")
        
        session.close()
        print("\n Everything is fully verified and working perfectly with Psycopg 3!")
        
    except Exception as e:
        print("ORM Query Test failed!")
        print(f"Error Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    verify_setup()