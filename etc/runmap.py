from AlertCypher import AlertCypher
import clinical.methods as rdas

db = AlertCypher('clinicaltest2')
rdas.condition_map(db)
