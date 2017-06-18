clean-db:
	docker-compose rm -f saifudb
run:
	echo "Will build core image first"
	cd core && make all && cd ..
	docker-compose up --abort-on-container-exit --build
all: clean-db run
