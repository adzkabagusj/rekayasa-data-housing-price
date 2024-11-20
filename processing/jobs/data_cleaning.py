import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging
from typing import Optional, Dict, List
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        # MongoDB configuration
        self.mongo_client = MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.mongo_client['rumah123']
        self.raw_listings = self.db['raw_listings']
        self.facilities = self.db['facilities']
        self.cleaned_listings = self.db['cleaned_listings']
        self.cleaned_facilities = self.db['cleaned_facilities']
        self.progress_collection = self.db['scraping_progress']

    def load_progress(self) -> Dict:
        """Load the current progress state from MongoDB."""
        try:
            progress = self.progress_collection.find_one({"_id": "current_progress"})
            if not progress:
                raise ValueError("No progress data found")
            return progress
        except Exception as e:
            logger.error(f"Failed to load progress: {e}")
            raise

    def clean_price(self, price: Optional[float]) -> Optional[int]:
        """Clean and convert price to integer."""
        try:
            if pd.isna(price) or price is None:
                return None
            return int(price)
        except:
            return None

    def clean_numeric(self, value: any, convert_to: str = 'int') -> Optional[any]:
        """Clean numeric values and convert to specified type."""
        try:
            if pd.isna(value) or value is None or value == '':
                return None
            if convert_to == 'int':
                return int(float(str(value).replace(',', '')))
            elif convert_to == 'float':
                return float(str(value).replace(',', ''))
            return value
        except:
            return None

    def clean_string(self, value: any) -> Optional[str]:
        """Clean string values."""
        try:
            if pd.isna(value) or value is None or value == '':
                return None
            return str(value).strip()
        except:
            return None

    def clean_rumah123_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform rumah123 raw data."""
        try:
            required_columns = [
                "judul_iklan", "harga", "kecamatan", "kabupaten_kota", "provinsi",
                "terakhir_diperbarui", "agen", "link_rumah123", "kamar_tidur",
                "kamar_mandi", "luas_tanah", "luas_bangunan", "carport",
                "sertifikat", "daya_listrik", "kamar_tidur_pembantu",
                "kamar_mandi_pembantu", "dapur", "ruang_makan", "ruang_tamu",
                "kondisi_perabotan", "material_bangunan", "material_lantai",
                "garasi", "jumlah_lantai", "konsep_dan_gaya_rumah", "pemandangan",
                "terjangkau_internet", "lebar_jalan", "tahun_dibangun",
                "tahun_direnovasi", "sumber_air", "hook", "kondisi_properti"
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column {col} is missing from data.")

            # Clean numeric columns
            numeric_int_columns = [
                "kamar_tidur", "kamar_mandi", "carport", "daya_listrik",
                "kamar_tidur_pembantu", "kamar_mandi_pembantu", "dapur",
                "garasi", "jumlah_lantai", "tahun_dibangun", "tahun_direnovasi"
            ]
            
            numeric_float_columns = ["luas_tanah", "luas_bangunan"]
            
            string_columns = [
                "kecamatan", "kabupaten_kota", "provinsi", "terakhir_diperbarui",
                "agen", "link_rumah123", "sertifikat", "ruang_makan", "ruang_tamu",
                "kondisi_perabotan", "material_bangunan", "material_lantai",
                "konsep_dan_gaya_rumah", "pemandangan", "terjangkau_internet",
                "lebar_jalan", "sumber_air", "hook", "kondisi_properti"
            ]

            # Apply cleaning functions
            df['harga'] = df['harga'].apply(self.clean_price)
            
            for col in numeric_int_columns:
                df[col] = df[col].apply(lambda x: self.clean_numeric(x, 'int'))
                
            for col in numeric_float_columns:
                df[col] = df[col].apply(lambda x: self.clean_numeric(x, 'float'))
                
            for col in string_columns:
                df[col] = df[col].apply(self.clean_string)

            return df
            
        except Exception as e:
            logger.error(f"Error cleaning rumah123 data: {str(e)}")
            raise

    def clean_osm_facilities_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform OpenStreetMap facilities data."""
        try:
            required_columns = [
                "kecamatan", "jumlah_fasilitas_pendidikan", 
                "jumlah_fasilitas_kesehatan", "jumlah_fasilitas_perbelanjaan",
                "jumlah_fasilitas_transportasi", "jumlah_fasilitas_rekreasi"
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column {col} is missing from data.")

            # Clean all facility counts as integers
            facility_columns = [col for col in required_columns if col != "kecamatan"]
            
            for col in facility_columns:
                df[col] = df[col].apply(lambda x: self.clean_numeric(x, 'int'))

            df['kecamatan'] = df['kecamatan'].apply(self.clean_string)
            
            return df
            
        except Exception as e:
            logger.error(f"Error cleaning OSM facilities data: {str(e)}")
            raise

    def process_current_page(self, progress: Dict) -> bool:
        """Process all data for the current pagination page."""
        current_page = progress["current_page"]
        logger.info(f"Processing data for page {current_page}")
        
        try:
            # 1. Get Rumah123 data for current page
            raw_listings = list(self.raw_listings.find({"page": current_page}))
            if not raw_listings:
                logger.warning(f"No raw listings found for page {current_page}")
                return False

            # Convert to DataFrame and clean
            listings_df = pd.DataFrame(raw_listings)
            cleaned_listings_df = self.clean_rumah123_data(listings_df)
            
            # Store cleaned listings
            cleaned_records = cleaned_listings_df.to_dict('records')
            if cleaned_records:
                self.cleaned_listings.insert_many(cleaned_records)
                logger.info(f"Stored {len(cleaned_records)} cleaned listings")

            # 2. Process facilities data
            unique_kecamatans = cleaned_listings_df['kecamatan'].unique()
            facilities_data = list(self.facilities.find({"kecamatan": {"$in": list(unique_kecamatans)}}))
            
            if facilities_data:
                facilities_df = pd.DataFrame(facilities_data)
                cleaned_facilities_df = self.clean_osm_facilities_data(facilities_df)
                
                # Store cleaned facilities
                cleaned_facilities = cleaned_facilities_df.to_dict('records')
                self.cleaned_facilities.insert_many(cleaned_facilities)
                logger.info(f"Stored {len(cleaned_facilities)} cleaned facilities records")

            # 3. Update progress
            for province in progress["provinces"]:
                if progress["provinces"][province] == current_page:
                    progress["provinces"][province] = current_page + 1
            progress["current_page"] = current_page + 1
            
            self.progress_collection.update_one(
                {"_id": "current_progress"},
                {"$set": progress}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing page {current_page}: {e}")
            return False

    def run(self):
        """Main execution flow."""
        try:
            # Load current progress
            progress = self.load_progress()
            
            # Process current page
            success = self.process_current_page(progress)
            
            if not success:
                logger.error("Failed to process current page")
                return
            
            logger.info("Successfully completed data cleaning pipeline")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
        finally:
            self.close()

    def close(self):
        """Close MongoDB connection."""
        if self.mongo_client:
            self.mongo_client.close()

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()