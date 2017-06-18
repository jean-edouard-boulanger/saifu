clean-db:
	docker-compose rm -f saifudb
run: core
	echo "Will build core image first"
	cd core && make all && cd ..
	docker-compose up --abort-on-container-exit --build
all: clean-db run
