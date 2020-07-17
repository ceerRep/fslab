#
# Students' Makefile for the Filesystem Lab
#
STUID = 2017000000-2017000001
#Leave no spaces between or after your STUID!!!
#If you finished this lab on your own, fill like this:2017000000-None
VERSION = 1
HANDINDIR = /home/handin-fs

MNTDIR = mnt
VDISK = vdisk

CC = gcc
CXX = g++
CFLAGS = -Wall -std=c11
CXXFLAGS = -Wall -std=gnu++17 -fno-rtti -fno-exceptions -Wno-sign-compare -Wno-reorder -Wno-unused-parameter

OBJS = disk.o fs.c

debug: umount clean fuse
	./fuse -s -f $(MNTDIR)

mount: umount clean fuse
	./fuse -s $(MNTDIR)

umount:
	-fusermount -u $(MNTDIR)

fuse: $(OBJS)
	echo $(abspath $(lastword $(MAKEFILE_LIST))) > fuse~
	rm -rf $(VDISK)/*
    ifeq ($(MNTDIR), $(wildcard $(MNTDIR)))
		rm -rf $(MNTDIR)
    endif
	mkdir $(MNTDIR)
	$(CC) $(CFLAGS) -g -rdynamic -o fuse $(OBJS) -DFUSE_USE_VERSION=29 -D_FILE_OFFSET_BITS=64 -lfuse #-lstdc++

fs.o: fs.cpp fs.c fs.ll
	$(CXX) $(CXXFLAGS) -Ofast -g -rdynamic -DDEBUG -c fs.cpp

fs.c: fs.s
	# ./llvm-cbe ./fs.ll -o fs.cbe.c
	# mv ./fs.cbe.c ./fs.c
	# sed -i "s/l_fptr_[0-9]\+\*/void*/g" fs.c
	# sed -i 's/uint32_t main(uint32_t \(.*\), uint8_t\*\* \(.*\))/int main(int \1, char** \2)/g' fs.c
	# sed -i 's/uint32_t main(uint32_t, uint8_t\*\*)/int main(int, char**)/g' fs.c
	# sed -i 's/__forceinline//g' fs.c
	# sed -it 's/__attribute__((always_inline)) inline/__attribute__(...)/g' fs.c
	python3 process.py < fs.s > fs.c
	

fs.ll: fs.cpp fs.hpp
	clang++-8 $(CXXFLAGS) -Ofast -S -emit-llvm fs.cpp

fs.s: fs.cpp fs.hpp
	$(CXX) $(CXXFLAGS) -Ofast -S fs.cpp

disk.o: disk.c disk.h

handin:
	chmod 600 fs.c
	add fs.c $(HANDINDIR)/$(STUID)-$(VERSION)-fs.c

clean:
	-rm -f *~ *.o fuse
	-rm -f fs.c fs.ll fs-opt.ll fs.s
	-rm -rf $(MNTDIR)
