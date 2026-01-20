from sqlalchemy import create_engine, text
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database_config import get_database_url


def fix_alembic_version(target_revision):
    """
    Updates the alembic_version table to the specified revision.
    """
    url = get_database_url()
    # Ensure usage of async driver format if necessary, but here we use sync for script
    # The config likely returns async url (postgresql+asyncpg), we need sync for this script usually or use async engine.
    # Let's try to convert to sync url if it is async
    if "postgresql+asyncpg" in url:
        url = url.replace("postgresql+asyncpg", "postgresql")

    print("Connecting to database...")
    engine = create_engine(url)

    with engine.connect() as connection:
        # Check current version
        try:
            result = connection.execute(text("SELECT version_num FROM alembic_version"))
            current_version = result.scalar()
            print(f"Current database version: {current_version}")
        except Exception as e:
            print(f"Could not read current version (table might be missing?): {e}")
            current_version = None

        print(f"Updating version to: {target_revision}")

        # Begin transaction
        with connection.begin():
            if current_version:
                connection.execute(
                    text("UPDATE alembic_version SET version_num = :ver"),
                    {"ver": target_revision},
                )
            else:
                connection.execute(
                    text("INSERT INTO alembic_version (version_num) VALUES (:ver)"),
                    {"ver": target_revision},
                )

        print("Success! Database version updated.")


if __name__ == "__main__":
    # We will pick the revision that looks like the latest one from the file list provided earlier
    # f8074233c2f6_add_should_record_data.py -> f8074233c2f6
    target_rev = "f8074233c2f6"
    fix_alembic_version(target_rev)
