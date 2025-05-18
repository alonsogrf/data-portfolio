# Script de conexión a Remotive API
import requests
import pandas as pd

url = "https://remotive.io/api/remote-jobs?search=data"
response = requests.get(url)
jobs = response.json()["jobs"]

df = pd.DataFrame(jobs)
df.to_csv("data/processed/remotive_jobs.csv", index=False)
