DIR := $(shell basename `pwd`)

$(DIR).jar: *.java build.gradle Makefile
	gradle build --stacktrace
	gradle shadowJar --stacktrace
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar input output

clean-output: 
	rm -rf output

clean: clean-output
	rm -rf build bin *.jar .gradle test.log
	clear
