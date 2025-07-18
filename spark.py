from pyspark.sql import SparkSession
import subprocess
import os

def find_beeline():
    print("🔍 Environment PATH:", os.environ.get("PATH", ""))
    print("🔍 HIVE_HOME:", os.environ.get("HIVE_HOME", ""))
    
    try:
        beeline_path = subprocess.check_output(["which", "beeline"]).decode().strip()
        real_path = subprocess.check_output(["readlink", "-f", beeline_path]).decode().strip()
        print(f"✅ 'beeline' found at: {beeline_path}")
        print(f"📎 Resolved real path: {real_path}")
    except Exception as e:
        print(f"❌ Could not find 'beeline': {e}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FindBeelinePath").getOrCreate()
    find_beeline()
    spark.stop()
