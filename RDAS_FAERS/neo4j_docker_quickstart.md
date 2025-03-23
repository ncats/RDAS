# Running Neo4j Using Docker

This guide explains how to run and manage Neo4j using Docker, along with setting up your Python environment for interacting with Neo4j.

---

## 1. Pull and Run Neo4j Docker Container

Pull the latest Neo4j Docker image:

```bash
docker pull neo4j
```

Run the container:

```bash
docker run --name myneo4j -p 7474:7474 -p 7687:7687 -d -e NEO4J_AUTH=neo4j/secretgraph neo4j:latest
```

**Explanation:**

- `--name myneo4j`: Names the container "myneo4j".
- `-p 7474:7474 -p 7687:7687`: Maps Neo4j's default ports (7474 for web interface, 7687 for Bolt protocol) to your local machine.
- `-d`: Runs the container in detached mode (background).
- `-e NEO4J_AUTH=neo4j/secretgraph`: Sets the initial username and password (`neo4j` / `secretgraph`).

---

## 2. Managing Neo4j Container

- Stop the container:

```bash
docker stop myneo4j
```

- Start the container again:

```bash
docker start myneo4j
```

---

## 3. Accessing Neo4j Data within Docker

Neo4j database files are stored within the Docker container.

- Enter the running container:

```bash
docker exec -it myneo4j bash
```

- Database files location inside the container (typically):

```bash
/var/lib/neo4j/data/databases
```

---

## 4. Neo4j Python Client Setup

- Set up your Python environment with the necessary packages:

```bash
pip install setuptools==60.9.3
pip install -U srt==3.5.2 neo4j==4.4.2
```

If you encounter errors during the upgrade, downgrade setuptools first:

```bash
pip install setuptools==60.8.2
pip install -U srt==3.5.2 neo4j==4.4.2
```

- Start Jupyter Lab (optional, for running Python scripts interactively):

```bash
jupyter lab
```

---

## 5. Connecting to Neo4j Using Python

Use the following Python script to connect and interact with Neo4j:

```python
from neo4j import GraphDatabase

# Connection details
uri = "neo4j://localhost:7687"
username = "neo4j"
password = "secretgraph"

# Example query
with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session() as session:
        result = session.run("MATCH (n:SafetyReport) RETURN n")
        for record in result:
            print(record)
```

---

## 6. Using Cypher-Shell within Docker

Enter Cypher-Shell inside the Docker container to run direct Cypher queries:

```bash
docker exec -it myneo4j bash
cypher-shell
```

Run example query:

```cypher
MATCH (n:SafetyReport) RETURN n;
```

---

## 7. Visualizing Database Schema

In Neo4j browser, run this command to visualize the schema:

```cypher
CALL db.schema.visualization()
```

---
## 8. Creating and Exporting Database Dumps with Docker

To create and export a Neo4j database dump from a Docker container: 

- List running containers to find the correct container ID or name:

```cypher
docker ps
```
- Stop the running Neo4j container (replace socialnetwork2 with your container name):

```cypher
docker stop socialnetwork2
```
- Create and export the dump to your local system:
```cypher
docker run --rm --volumes-from socialnetwork2 -v C:/Users/valinejadj2/Desktop:/backups neo4j:latest neo4j-admin database dump neo4j --to-path=/backups/
```

This command creates a neo4j.dump file on your local desktop (C:/Users/valinejadj2/Desktop). You can verify the dump file afterward by checking the specified folder.

---

You're all set to use Neo4j with Docker!
