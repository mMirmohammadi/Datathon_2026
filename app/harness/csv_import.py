from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from collections.abc import Iterable

from app.participant.listing_row_parser import prepare_listing_row


def create_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
            listing_id TEXT PRIMARY KEY,
            platform_id TEXT,
            scrape_source TEXT,
            title TEXT NOT NULL,
            description TEXT,
            street TEXT,
            city TEXT,
            postal_code TEXT,
            canton TEXT,
            price INTEGER,
            rooms REAL,
            area REAL,
            available_from TEXT,
            latitude REAL,
            longitude REAL,
            distance_public_transport INTEGER,
            distance_shop INTEGER,
            distance_kindergarten INTEGER,
            distance_school_1 INTEGER,
            distance_school_2 INTEGER,
            feature_balcony INTEGER,
            feature_elevator INTEGER,
            feature_parking INTEGER,
            feature_garage INTEGER,
            feature_fireplace INTEGER,
            feature_child_friendly INTEGER,
            feature_pets_allowed INTEGER,
            feature_temporary INTEGER,
            feature_new_build INTEGER,
            feature_wheelchair_accessible INTEGER,
            feature_private_laundry INTEGER,
            feature_minergie_certified INTEGER,
            features_json TEXT NOT NULL,
            offer_type TEXT,
            object_category TEXT,
            object_type TEXT,
            original_url TEXT,
            images_json TEXT,
            location_address_json TEXT,
            orig_data_json TEXT,
            raw_json TEXT NOT NULL
        )
        """
    )
    connection.commit()


def import_csvs(connection: sqlite3.Connection, csv_paths: Iterable[Path]) -> None:
    for csv_path in csv_paths:
        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = [prepare_listing_row(row) for row in reader]

            connection.executemany(
                """
                INSERT INTO listings (
                    listing_id,
                    platform_id,
                    scrape_source,
                    title,
                    description,
                    street,
                    city,
                    postal_code,
                    canton,
                    price,
                    rooms,
                    area,
                    available_from,
                    latitude,
                    longitude,
                    distance_public_transport,
                    distance_shop,
                    distance_kindergarten,
                    distance_school_1,
                    distance_school_2,
                    feature_balcony,
                    feature_elevator,
                    feature_parking,
                    feature_garage,
                    feature_fireplace,
                    feature_child_friendly,
                    feature_pets_allowed,
                    feature_temporary,
                    feature_new_build,
                    feature_wheelchair_accessible,
                    feature_private_laundry,
                    feature_minergie_certified,
                    features_json,
                    offer_type,
                    object_category,
                    object_type,
                    original_url,
                    images_json,
                    location_address_json,
                    orig_data_json,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    connection.commit()


def create_indexes(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_postal_code ON listings(postal_code)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_canton ON listings(canton)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_rooms ON listings(rooms)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_latitude ON listings(latitude)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_listings_longitude ON listings(longitude)")
    connection.commit()
