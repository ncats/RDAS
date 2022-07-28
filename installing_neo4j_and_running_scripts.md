# Installation
Followed reference [here](https://neo4j.com/docs/operations-manual/current/installation/linux/debian/)
- Install Java 11 JRE: `sudo apt-get update && sudo apt-get install openjdk-11-jre`
- Add neo4j repository:
  ```sh
  wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
  echo 'deb https://debian.neo4j.com stable 4.4' | sudo tee -a /etc/apt/sources.list.d/neo4j.list
  sudo apt-get update
  ```
 - Install neo4j community edition (can check whatever server you want to copy, e.g. rdip2, to decide which version to install.) `sudo apt-get install neo4j=1:4.4.4`
 - Can start neo4j now using `sudo systemctl start neo4j`. After a short (few seconds) delay, `sudo neo4j status` will indicate that neo4j is running.
   - To test out everything is working correctly, use `cypher-shell` and try to connect with username `neo4j`, password `neo4j`. It should ask you to change the default password if it has connected successfully. 

# Copying another database into this new installation
Followed reference [here](https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/) and [here](https://neo4j.com/docs/operations-manual/current/backup-restore/restore-dump/)
- You first need to create a dump from the database you wish to copy. You will need `sudo` permissions on that original instance to be able to create the dump, or ask someone with sudo permissions to do it.
  - Shut down the database you wish to copy with `sudo neo4j stop`, and create the dump with `sudo neo4j-admin dump --database=neo4j --to=~/<name of dump>.dump`. The dump should be a single file with `.dump` extension, as you specified it in the command.
- Copy the dump file from that original instance to the instance you wish to set up the copy on.
  - For me, I could not `scp` directly between the two instances (Host key verification failed, connection lost), so I first copied it to my local machine, and then copied it to my new instance:
    ```sh
    scp username@originalinstance:/path/to/dump/file.dump ~/dumpcopy.dump
    scp ~/dumpcopy.dump username@newinstance:/path/to/put/file/at.dump
    ```
- On your new instance, shut down Neo4j with `sudo neo4j stop`.
- Load the file with `sudo neo4j-admin load --from=path/to/dump.dump --database=neo4j --force`
- Restart the instance with `sudo systemctl start neo4j`. The database should have been installed correctly.
  - You can connect to the db with cypher-shell and do a few test queries to make sure the data looks right.

# Running Python scripts on the instance
- Check python3 is installed (it normally comes with the Linux distribution)
- Install pip with `sudo apt install python3-pip`.
- Use your favorite text editor to add your .local/bin directory to $PATH (append `export PATH="/home/<your username>/.local/bin:$PATH"` to the end of `.bashrc`)
- Copy the file you wish to run from your computer with `scp`, or clone it from git using `git clone`.
- I recommend installing `virtualenv` and creating a new virtual environment to avoid package conflicts.
  ```
  pip install virtualenv
  virtualenv venv
  source venv/bin/activate
  ```
- Install the relevant Python packages on the instance; i.e. `pip install neo4j==4.4.4`. If you are unsure which version of package to install, use `pip show` on your own computer to determine package version.
- Edit your `config.ini` or code to connect to the right neo4j url, e.g. localhost:7687, with the password you just reset when connecting to the new neo4j installation with cypher-shell.
- Run using `python3 <your script>.py`.
- If your script takes a long time to run, I recommend using `nohup`, for example `nohup command to run > log.out 2>&1 &`. This will prevent it from breaking when you disconnect from ssh.

# Troubleshooting
- If `apt-get` is not an available command, you'll need to follow a different installation process, probably installing using RPM package.
- If there is no such package `openjdk-11-jre`, you may be on an older version of Debian. In that case you'll need to add the Java 11 PPA in order to install.
- If cypher-shell says "Connection refused" or "Database is not running at xxxx",  there is probably some issue with the installation or config file; try `sudo apt-get purge neo4j` and reinstalling
- Running `neo4j status` without `sudo` may incorrectly return "Neo4j is not running". Always use sudo to check neo4j status.
- If `sudo neo4j stop` outputs `Stopping Neo4j................` and goes on forever, you can force kill the Neo4j process. Find the PID using `sudo neo4j status` (if neo4j was not stopped successfully, it should say `Neo4j is running at pid xxxxxx`) and then kill it using `sudo kill -9 <pid>`.
- If, after `neo4j-admin load`, cypher-shell refuses to connect to the database, something might have went wrong during the load. Try reloading by running that load command again.
- If your scripts are mysteriously failing, check the neo4j log file (`cat /var/log/neo4j/neo4j.log | less`).
- If the neo4j log file contains Java exception `java.lang.OutOfMemoryError: Java heap space`, you need to manually increase the heap size that neo4j uses. Edit /etc/neo4j/neo4j.conf and set `dbms.memory.heap.initial_size=3000m` (or anything larger), `dbms.memory.heap.max_size=5000m`.
