import methods as rdas
from AlertCypher import AlertCypher

db = AlertCypher('clinicaltest')
rdas.condition_map(db)
