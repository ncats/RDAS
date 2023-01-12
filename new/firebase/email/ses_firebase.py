import ses

# at the end of each neo4j update retrieve list of all users and their GARD subscriptions of the specific update neo4j from firebase 
# (if clinical just updated, only get subscriptions that have clinical trials subbed)

# Get a unique list of all of the users subbed GARD IDs under associated subscription (clinical trial subscriptions)
# Check each unique subbed GARD in neo4j for new added nodes (would have DateCreated equal to current date) ----- add last update field

# Log the count of new added nodes for each GARD in a dictionary
# Compare dictionary back to user subscriptions to get user specific data
# For each user send a single email notifying them of all the added nodes (and how many) under their subscribed GARD

def trigger_email(type):
    if type == "clinical":
        # gather firebase data and neo4j data
        ses.clinical_msg() # pass in data for email as dict
    elif type == "pubmed":
        ses.pubmed_msg()
    elif type == "grant":
        ses.grant_msg()
