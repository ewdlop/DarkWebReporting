import sqlite3
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional
import logging

class ArachnidDatabase:
    def __init__(self, db_name: str = "arachnid_database.db"):
        """Initialize the database connection and create necessary tables."""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_tables()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _create_tables(self):
        """Create the species and sightings tables if they don't exist."""
        # Species table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS species (
            id INTEGER PRIMARY KEY,
            scientific_name TEXT UNIQUE NOT NULL,
            common_name TEXT,
            family TEXT,
            venomous BOOLEAN,
            average_size_mm FLOAT,
            habitat TEXT,
            description TEXT
        )
        ''')

        # Sightings table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS sightings (
            id INTEGER PRIMARY KEY,
            species_id INTEGER,
            latitude FLOAT,
            longitude FLOAT,
            date_time TIMESTAMP,
            location_description TEXT,
            weather_conditions TEXT,
            notes TEXT,
            photo_path TEXT,
            FOREIGN KEY (species_id) REFERENCES species (id)
        )
        ''')
        self.conn.commit()

    def add_species(self, species_data: Dict) -> int:
        """
        Add a new species to the database.
        
        Args:
            species_data: Dictionary containing species information
            
        Returns:
            id: The ID of the newly inserted species
        """
        try:
            query = '''
            INSERT INTO species (
                scientific_name, common_name, family, venomous,
                average_size_mm, habitat, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                species_data['scientific_name'],
                species_data.get('common_name'),
                species_data.get('family'),
                species_data.get('venomous', False),
                species_data.get('average_size_mm'),
                species_data.get('habitat'),
                species_data.get('description')
            )
            self.cursor.execute(query, values)
            self.conn.commit()
            self.logger.info(f"Added new species: {species_data['scientific_name']}")
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            self.logger.error(f"Species {species_data['scientific_name']} already exists")
            return -1

    def record_sighting(self, sighting_data: Dict) -> int:
        """
        Record a new arachnid sighting.
        
        Args:
            sighting_data: Dictionary containing sighting information
            
        Returns:
            id: The ID of the newly recorded sighting
        """
        try:
            query = '''
            INSERT INTO sightings (
                species_id, latitude, longitude, date_time,
                location_description, weather_conditions, notes, photo_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (
                sighting_data['species_id'],
                sighting_data.get('latitude'),
                sighting_data.get('longitude'),
                sighting_data.get('date_time', datetime.now()),
                sighting_data.get('location_description'),
                sighting_data.get('weather_conditions'),
                sighting_data.get('notes'),
                sighting_data.get('photo_path')
            )
            self.cursor.execute(query, values)
            self.conn.commit()
            self.logger.info(f"Recorded new sighting for species ID: {sighting_data['species_id']}")
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.logger.error(f"Error recording sighting: {e}")
            return -1

    def get_species_statistics(self) -> pd.DataFrame:
        """
        Get statistics about species sightings.
        
        Returns:
            DataFrame containing species statistics
        """
        query = '''
        SELECT 
            s.scientific_name,
            s.common_name,
            COUNT(st.id) as sighting_count,
            MIN(st.date_time) as first_sighting,
            MAX(st.date_time) as last_sighting
        FROM species s
        LEFT JOIN sightings st ON s.id = st.species_id
        GROUP BY s.id
        '''
        return pd.read_sql_query(query, self.conn)

    def search_sightings(self, 
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        species_name: Optional[str] = None,
                        location: Optional[str] = None) -> pd.DataFrame:
        """
        Search sightings based on various criteria.
        
        Args:
            start_date: Start date for filtering sightings
            end_date: End date for filtering sightings
            species_name: Scientific or common name of the species
            location: Location description to search for
            
        Returns:
            DataFrame containing filtered sightings
        """
        query = '''
        SELECT 
            st.*,
            s.scientific_name,
            s.common_name
        FROM sightings st
        JOIN species s ON st.species_id = s.id
        WHERE 1=1
        '''
        params = []

        if start_date:
            query += " AND st.date_time >= ?"
            params.append(start_date)
        if end_date:
            query += " AND st.date_time <= ?"
            params.append(end_date)
        if species_name:
            query += " AND (s.scientific_name LIKE ? OR s.common_name LIKE ?)"
            params.extend([f"%{species_name}%", f"%{species_name}%"])
        if location:
            query += " AND st.location_description LIKE ?"
            params.append(f"%{location}%")

        return pd.read_sql_query(query, self.conn, params=params)

    def export_data(self, file_path: str, format: str = 'csv'):
        """
        Export all data to a file.
        
        Args:
            file_path: Path to save the exported data
            format: Export format ('csv' or 'excel')
        """
        # Get all data with joins
        query = '''
        SELECT 
            st.id as sighting_id,
            s.scientific_name,
            s.common_name,
            s.family,
            s.venomous,
            s.average_size_mm,
            s.habitat,
            st.latitude,
            st.longitude,
            st.date_time,
            st.location_description,
            st.weather_conditions,
            st.notes
        FROM sightings st
        JOIN species s ON st.species_id = s.id
        '''
        df = pd.read_sql_query(query, self.conn)
        
        if format.lower() == 'csv':
            df.to_csv(file_path, index=False)
        elif format.lower() == 'excel':
            df.to_excel(file_path, index=False)
        else:
            raise ValueError("Format must be either 'csv' or 'excel'")
        
        self.logger.info(f"Data exported to {file_path}")

    def __del__(self):
        """Close the database connection when the object is destroyed."""
        self.conn.close()

# Example usage:
if __name__ == "__main__":
    # Initialize database
    db = ArachnidDatabase()
    
    # Add a sample species
    sample_species = {
        "scientific_name": "Latrodectus mactans",
        "common_name": "Southern black widow",
        "family": "Theridiidae",
        "venomous": True,
        "average_size_mm": 8.0,
        "habitat": "Human structures, woodpiles, rocky areas",
        "description": "Female is shiny black with red hourglass marking"
    }
    
    species_id = db.add_species(sample_species)
    
    # Record a sample sighting
    if species_id != -1:
        sample_sighting = {
            "species_id": species_id,
            "latitude": 34.0522,
            "longitude": -118.2437,
            "location_description": "Garden shed",
            "weather_conditions": "Warm, dry",
            "notes": "Female with egg sac"
        }
        
        db.record_sighting(sample_sighting)
    
    # Get and print statistics
    print("\nSpecies Statistics:")
    print(db.get_species_statistics())
    
    # Search for recent sightings
    print("\nRecent Sightings:")
    recent_sightings = db.search_sightings(
        start_date=datetime(2024, 1, 1),
        species_name="widow"
    )
    print(recent_sightings)
