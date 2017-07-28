dist:
	make -C ./data dist
	make -C ./account dist
	make -C ./commitlog dist
	make -C ./gateway dist
	make -C ./project dist
	make -C ./nodes dist

docker:
	make -C ./api docker
	make -C ./data docker
	make -C ./account docker
	make -C ./commitlog docker
	make -C ./gateway docker
	make -C ./project docker
	make -C ./nodes docker

docker-push:
	make -C ./api docker-push
	make -C ./data docker-push
	make -C ./account docker-push
	make -C ./commitlog docker-push
	make -C ./gateway docker-push
	make -C ./project docker-push
	make -C ./nodes docker-push
