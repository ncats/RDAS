* Must be in base folder before running any command (ex. alertremake)

# Manual Operation
For updating and creating databases manually
## Example usage [foreground]
`python3 driver_manual.py -db {DATABASE TAG} -m {MODE} -r {RESTART FROM}`
## Example usage [background]
`nohup python3 -u driver_manual.py -db {DATABASE TAG} -m {MODE} -r {RESTART FROM} &`
## Arguments
### Tags
* ct [Clinical Trial Database]
* pm [PubMed Database]
* gnt [NIH Funded Projects Database]
### Modes
* create
* update
### Restart from
* 4 [Database population]
* 3 [Query generation]
* 2 [Full trial data gathering]
* 1 [NCTID gathering]
* 0 [Rare disease name webscraping; from scratch]


# Automatic Operation
For updating databases automatically
This can be paired with the linux `nohup` command to run in the background
## Example usage [foreground]
`python3 driver_automatic.py -db {DATABASE TAG}`
## Example usage [background]
`nohup python3 -u driver_automatic.py -db {DATABASE TAG} &`
This will redirect all console output to a `nohup.txt` in the base directory
## Arguments
### Tags
* ct [Clinical Trial Database]
* pm [PubMed Database]
* gnt [NIH Funded Projects Database]
