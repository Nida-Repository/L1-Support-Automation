from app.database import SessionLocal
from app.models import Site, Sensor

db = SessionLocal()

print("\nTesting Site")
site = db.query(Site).first()
print(site)

print("\nTesting Sensor")
sensor = db.query(Sensor).first()
print(sensor)

db.close()