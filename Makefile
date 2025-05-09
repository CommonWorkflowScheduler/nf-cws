
config ?= compileClasspath

ifdef module 
mm = :${module}:
else 
mm = 
endif 

C_SCRIPT_SRC = plugins/nf-cws/src/resources/nf-cws/getStatsAndResolveSymlinks.c
C_SCRIPT_AARCH64 = plugins/nf-cws/src/resources/nf-cws/getStatsAndResolveSymlinks_linux_aarch64
C_SCRIPT_X86_64 = plugins/nf-cws/src/resources/nf-cws/getStatsAndResolveSymlinks_linux_x86_64
C_SCRIPT_ALL_TARGETS = $(C_SCRIPT_X86_64) $(C_SCRIPT_AARCH64)

arch:=$(shell uname -m)

ifneq ($(arch),x86_64)
$(info ====================================================================================)
$(info This Makefile assumes to be run on an x86_64 build system. Found: $(arch))
$(info As an alternative you can adjust the Makefile or build $(C_SCRIPT_SRC) yourself for the following targets:)
$(info - target aarch64-linux-gnu: saved to $(C_SCRIPT_AARCH64))
$(info - target x86_64-linux-gnu: saved to $(C_SCRIPT_X86_64))
$(error Aborting)
endif

$(C_SCRIPT_AARCH64): $(C_SRIPT_SRC)
	clang -static \
		-target aarch64-linux-gnu \
		--sysroot=/usr/aarch64-linux-gnu \
		plugins/nf-cws/src/resources/nf-cws/getStatsAndResolveSymlinks.c \
		-fuse-ld=lld \
		-o $@


$(C_SCRIPT_X86_64): $(C_SCRIPT_SRC)
	clang -static \
		-target x86_64-linux-gnu \
		plugins/nf-cws/src/resources/nf-cws/getStatsAndResolveSymlinks.c \
		-fuse-ld=lld \
		-o $@

clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	rm -rf plugins/*/build
	rm $(C_SCRIPT_ALL_TARGETS)
	./gradlew clean

compile:
	./gradlew :nextflow:exportClasspath compileGroovy
	@echo "DONE `date`"


check:
	./gradlew check


#
# Show dependencies try `make deps config=runtime`, `make deps config=google`
#
deps:
	./gradlew -q ${mm}dependencies --configuration ${config}

deps-all:
	./gradlew -q dependencyInsight --configuration ${config} --dependency ${module}

#
# Refresh SNAPSHOTs dependencies
#
refresh:
	./gradlew --refresh-dependencies

#
# Run all tests or selected ones
#
test:
ifndef class
	./gradlew ${mm}test
else
	./gradlew ${mm}test --tests ${class}
endif

assemble:
	./gradlew assemble

#
# generate build zips under build/plugins
# you can install the plugin copying manually these files to $HOME/.nextflow/plugins
#
buildPlugins: $(C_SCRIPT_ALL_TARGETS)
	./gradlew copyPluginZip

#
# Upload JAR artifacts to Maven Central
#
upload:
	./gradlew upload


upload-plugins:
	./gradlew plugins:upload

publish-index:
	./gradlew plugins:publishIndex
