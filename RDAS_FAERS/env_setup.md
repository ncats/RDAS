# Quickstart Guide for RDAS Dev

This guide walks you through using the RDAS development environment and connecting to the Neo4j database.

---

## 1. Connecting to RDAS Dev

Use an SSH client to connect via SSH:

```bash
ssh your_username@rdas-dev.example.com
```

This server is recommended for running your Python code.

---

## 2. Activate RDAS Environment

Once logged in, activate the `rdas` Conda environment:

```bash
conda activate rdas
```

You can now run your Python code. The `rdas` environment includes the Neo4j Python package.

---

## 3. Accessing the Neo4j Database

The Neo4j database server is separate from RDAS Dev. Connect using the following string in your Python scripts:

```python
bolt://neo4j-dev.example.com:7687
```

---

## 4. Edit Your `.bashrc` File

Ensure Conda is properly configured by editing your `.bashrc`:

- Open the file:

```bash
vi ~/.bashrc
```

- Append this code at the end:

```bash
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/opt/conda/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
  eval "$__conda_setup"
else
  if [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then
     . "/opt/conda/etc/profile.d/conda.sh"
  else
    export PATH="/opt/conda/bin:$PATH"
  fi
fi
unset __conda_setup
# <<< conda initialize <<<
```

- Save and exit the file.

- Apply the changes:

```bash
source ~/.bashrc
```

- Verify Conda installation:

```bash
which conda
```

---

## 5. Alternative Conda Environment Setup

Alternatively, manually set up your Conda environment as follows:

- Activate a specific Conda version:

```bash
source /path/to/miniconda/bin/activate
```

- Create or remove an environment:

```bash
# Create environment named myenv
conda create --name myenv

# Remove environment named social_network
conda env remove -n social_network
```

- Create an environment with a specific Python version:

```bash
conda create --prefix ~/social_network python=3.9
```

- Verify Conda installation:

```bash
which conda
```

- Activate the environment and install Neo4j:

```bash
conda activate social_network
pip3 install neo4j
```

---

## 6. Running and Managing Python Scripts

Useful commands for managing Python scripts:

- Edit your Python script:

```bash
vi script.py
```

- Check running Python processes:

```bash
ps -ef | grep python
```

- Kill a specific process:

```bash
kill -9 <process_id>
```

- Run script in the background (keeps running after session closes):

```bash
nohup python3 script.py > script.log 2>&1 &
```

- View logs:

```bash
cat script.log
```

- Keep session alive:

```bash
while true; do echo "Keeping session alive..."; sleep 300; done
```

---

## 7. Quickly Killing a Process

Kill processes by name quickly with:

```bash
pkill -9 -f script.py
```

---

## Additional Resources

- [Anaconda Documentation](https://docs.anaconda.com/)
- [YouTube Guide on Conda](https://www.youtube.com/watch?v=kLqfH3Euiwg)
