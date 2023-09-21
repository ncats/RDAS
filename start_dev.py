# start automatic_driver for each of the databases
# when an update is finished: trigger generate_dump within the automatic_driver for that specific database
# generate_dump will create a dump file into the backup folder and then copy that file to the transfer folder
# file_transfer will trigger and get the dump file in the transfer folder and sent it to the test servers transfer folder


