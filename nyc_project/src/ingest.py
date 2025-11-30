#%%
# ingesting the data source to destination
import requests
from pathlib import Path



def download_data(url: str, target_dir: Path):
    if not Path(target_dir).exists():
        target_dir.mkdir(parents=True, exist_ok=True) 

    print(f"Downloading dataset from:\n{url}")
    response = requests.get(url)
    if response.status_code != 200:
        return print("Failed to download. HTTP Status:", response.status_code)
        
    
    with open(target_dir/"yellow_2023_01.parquet", "wb") as f:
        f.write(response.content)
    print(f"File downloaded to: {target_dir}")
    

if __name__ == "__main__":
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    root_path = Path(__file__).parents[1]

    print(f"root path : {root_path}")

    target_ = root_path / "data/raw"

    download_data(URL, target_dir=target_)
# %%
