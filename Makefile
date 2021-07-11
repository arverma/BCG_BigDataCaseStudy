build:
	rm -rf Dist
	mkdir Dist
	cp Code/main.py ./Dist
	cp Code/config.yaml ./Dist
	unzip Data.zip -d Dist/
	cd Code/src && zip -r ../../dist/src.zip .
	rm -rf Dist/__MACOSX
