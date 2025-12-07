PYTHON = python3
PROJECT_DIR := $(shell pwd)

node1:
	cd "$(PROJECT_DIR)" && $(PYTHON) node.py 1

node2:
	cd "$(PROJECT_DIR)" && $(PYTHON) node.py 2

node3:
	cd "$(PROJECT_DIR)" && $(PYTHON) node.py 3

node4:
	cd "$(PROJECT_DIR)" && $(PYTHON) node.py 4

node5:
	cd "$(PROJECT_DIR)" && $(PYTHON) node.py 5

all:
	@echo "Starting all 5 nodes..."
	@osascript -e 'tell application "Terminal" to do script "cd \"$(PROJECT_DIR)\"; $(PYTHON) node.py 1"'
	@osascript -e 'tell application "Terminal" to do script "cd \"$(PROJECT_DIR)\"; $(PYTHON) node.py 2"'
	@osascript -e 'tell application "Terminal" to do script "cd \"$(PROJECT_DIR)\"; $(PYTHON) node.py 3"'
	@osascript -e 'tell application "Terminal" to do script "cd \"$(PROJECT_DIR)\"; $(PYTHON) node.py 4"'
	@osascript -e 'tell application "Terminal" to do script "cd \"$(PROJECT_DIR)\"; $(PYTHON) node.py 5"'

clean:
	rm -f blockchain_*.json balances_*.json paxos_state_*.json
	@echo "Cleaned all persistent files."
