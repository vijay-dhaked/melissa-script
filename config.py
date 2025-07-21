from dotenv import dotenv_values

env_vars = dotenv_values(".env")

DB_URI = env_vars.get("DB_URI")
MELISSA_KEY = env_vars.get("MELISSA_KEY")