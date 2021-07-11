build:
	rm -rf Dist
	mkdir Dist
	cp Code/main.py ./Dist
	cp Code/config.yaml ./Dist
	cp -R Data ./Dist/
	cd Code/src && zip -r ../../dist/src.zip .
