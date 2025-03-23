# 1️⃣ Use the official Neo4j image
FROM neo4j:5.10

# 2️⃣ Set the working directory inside the container
WORKDIR /home/neo4j

# 3️⃣ Update system and install essential Linux packages
RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y curl bash-completion

# 4️⃣ Set Neo4j environment variables (authentication, memory, etc.)
ENV NEO4J_AUTH=neo4j/yourpassword
ENV NEO4J_dbms_memory_heap_initial__size=512m
ENV NEO4J_dbms_memory_heap_max__size=2G
ENV NEO4J_dbms_memory_pagecache_size=2G

# 5️⃣ Install additional plugins (APOC, Graph Data Science, etc.)
RUN mkdir -p /var/lib/neo4j/plugins
RUN curl -L https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/5.10.0/apoc-5.10.0-core.jar -o /var/lib/neo4j/plugins/apoc.jar
RUN curl -L https://dist.neo4j.org/gds/gds-2.5.3-standalone.jar -o /var/lib/neo4j/plugins/gds.jar

# 6️⃣ Expose Neo4j ports (Web UI + Database Connection)
EXPOSE 7474 7687

# 7️⃣ Set the default command to run Neo4j
CMD ["neo4j"]
