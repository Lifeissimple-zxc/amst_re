import yaml

with open(file="config/main_config.yaml", encoding="utf-8") as _f:
    MAIN_CFG = yaml.safe_load(stream=_f)
with open(file="secrets/secrets.yaml", encoding="utf-8") as _f:
    SECRETS = yaml.safe_load(stream=_f)
with open(file=MAIN_CFG["db"]["create_sql"], encoding="utf-8") as _f:
    _create_sql_string = _f.read()
    CREATE_SQL_STATEMENTS = _create_sql_string.split(";")