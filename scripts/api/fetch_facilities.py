import requests
import json
import time
import logging
import pandas as pd
from typing import Optional, Dict, Set, List
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('osm_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OSMFacilitiesFetcher:
    def __init__(self):
        # MongoDB configuration
        self.mongo_client = MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.mongo_client['rumah123']
        self.raw_collection = self.db['raw_listings']
        self.facilities_collection = self.db['facilities']
        self.progress_collection = self.db['scraping_progress']
        
        self.overpass_url = "http://overpass-api.de/api/interpreter"
        
        # Define facility queries
        self.facility_queries = {
            "jumlah_fasilitas_pendidikan": [
                'amenity~"school|university|college|kindergarten"'
            ],
            "jumlah_fasilitas_kesehatan": [
                'amenity~"hospital|clinic|doctors|dentist|pharmacy"'
            ],
            "jumlah_fasilitas_perbelanjaan": [
                'shop~"supermarket|mall|department_store|convenience"',
                'amenity~"marketplace|shopping_mall"'
            ],
            "jumlah_fasilitas_transportasi": [
                'amenity~"bus_station|taxi|ferry_terminal"',
                'aeroway~"aerodrome|terminal"',
                'railway~"station|halt"'
            ],
            "jumlah_fasilitas_rekreasi": [
                'leisure~"park|sports_centre|fitness_centre|swimming_pool"',
                'amenity~"park|theatre|cinema"'
            ]
        }

        # Setup session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def build_query(self, kecamatan: str, facility_filter: str) -> str:
        """Build Overpass API query for a specific kecamatan and facility type."""
        query = f"""
        [out:json][timeout:60];
        area["name"="Indonesia"]->.country;
        area["admin_level"="6"]["name"="{kecamatan}"](area.country)->.searchArea;
        (
          nwr[{facility_filter}](area.searchArea);
        );
        out count;
        """
        return query.strip()

    def get_facilities_count(self, kecamatan: str, max_retries: int = 3) -> Optional[Dict]:
        """
        Fetch facility counts for a given kecamatan with improved error handling
        and rate limiting.
        """
        results = {}
        retry_count = 0
        
        for category, facility_filters in self.facility_queries.items():
            total_count = 0
            
            for facility_filter in facility_filters:
                query = self.build_query(kecamatan, facility_filter)
                
                while retry_count < max_retries:
                    try:
                        # Add delay to respect rate limits
                        time.sleep(2)
                        
                        response = self.session.post(
                            self.overpass_url,
                            data=query,
                            timeout=60,
                            headers={'Content-Type': 'application/x-www-form-urlencoded'}
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            if 'elements' in data:
                                count = len(data['elements'])
                                total_count += count
                            break
                        elif response.status_code == 429:  # Too Many Requests
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited. Waiting {wait_time} seconds")
                            time.sleep(wait_time)
                            retry_count += 1
                        else:
                            logger.error(f"API error {response.status_code}: {response.text}")
                            retry_count += 1
                            time.sleep(5)
                    
                    except Exception as e:
                        logger.error(f"Exception during API call for {kecamatan}: {e}")
                        retry_count += 1
                        time.sleep(5)
                        
                if retry_count >= max_retries:
                    logger.error(f"Max retries reached for {kecamatan}")
                    return None
                    
            results[category] = total_count
            
        # Add metadata
        results.update({
            "kecamatan": kecamatan,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        })
        
        return results


    def get_current_kecamatans(self, progress: Dict) -> Set[str]:
        """Extract unique kecamatan names from the current pagination page."""
        current_page = progress["current_page"]
        kecamatans = set()

        try:
            # Query MongoDB for kecamatans from the current page
            pipeline = [
                {
                    "$match": {
                        "page": current_page,
                        "kecamatan": {"$exists": True, "$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": "$kecamatan"
                    }
                }
            ]
            
            results = self.raw_collection.aggregate(pipeline)
            kecamatans = {doc["_id"] for doc in results}
            
        except Exception as e:
            logger.error(f"Error fetching kecamatans: {e}")

        return kecamatans

    def save_facilities_to_mongodb(self, facilities: Dict) -> bool:
        """Save facilities data to MongoDB."""
        try:
            kecamatan = facilities["kecamatan"]
            
            # Add timestamp and update if exists
            facilities['updated_at'] = datetime.now()
            
            # Upsert the facilities data
            result = self.facilities_collection.update_one(
                {"kecamatan": kecamatan},
                {"$set": facilities},
                upsert=True
            )
            
            logger.info(f"Successfully saved facilities data for {kecamatan}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save facilities data: {e}")
            return False

    def facility_exists(self, kecamatan: str) -> bool:
        """Check if facilities data already exists for a kecamatan."""
        return self.facilities_collection.find_one({"kecamatan": kecamatan}) is not None

    def run(self):
        """Main execution flow."""
        try:
            # Get current progress
            progress = self.progress_collection.find_one({"_id": "current_progress"})
            if not progress:
                logger.error("No progress data found")
                return

            kecamatans = self.get_current_kecamatans(progress)
            logger.info(f"Found {len(kecamatans)} unique kecamatans on page {progress['current_page']}")

            for kecamatan in kecamatans:
                # Skip if facilities already exist
                if self.facility_exists(kecamatan):
                    logger.info(f"Facilities for {kecamatan} already exist, skipping")
                    continue

                logger.info(f"Fetching facilities for {kecamatan}")
                facilities = self.get_facilities_count(kecamatan)
                
                if facilities:
                    self.save_facilities_to_mongodb(facilities)
                else:
                    logger.error(f"Failed to fetch facilities for {kecamatan}")

        except Exception as e:
            logger.error(f"Error in run method: {e}")
        finally:
            self.close()

    def close(self):
        """Close MongoDB connection."""
        if self.mongo_client:
            self.mongo_client.close()



def verify_environment():
    """Verify all required environment variables are set."""
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        raise EnvironmentError("MONGODB_URI environment variable is not set")
    
    # Test MongoDB connection
    try:
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Will raise exception if cannot connect
        logger.info("Successfully connected to MongoDB")
    except Exception as e:
        raise EnvironmentError(f"Failed to connect to MongoDB: {e}")

# Add this at the start of each script's main execution:
if __name__ == "__main__":
    try:
        verify_environment()
        fetcher = OSMFacilitiesFetcher()
        fetcher.run()
    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise  # This will ensure the subprocess.run() captures the error
    finally:
        fetcher.close()