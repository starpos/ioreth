.PHONY: iores ioth clean

CXX = g++
CFLAGS = -Wall -O2 -std=c++11 -pthread

all: iores ioth

iores: iores.o
	$(CXX) $(CFLAGS) -o $@ $<
ioth: ioth.o
	$(CXX) $(CFLAGS) -o $@ $<

.cpp.o:
	$(CXX) $(CFLAGS) -c $<

iores.o: iores.cpp util.hpp ioreth.hpp
ioth.o: ioth.cpp util.hpp ioreth.hpp thread_pool.hpp

clean: cleanTest
	rm -f iores ioth *.o

# for test.
sample_thread_pool.o: sample_thread_pool.cpp thread_pool.hpp
sample_thread_pool: sample_thread_pool.o
	$(CXX) $(CFLAGS) -o $@ $<

cleanTest:
	rm -f sample_thread_pool
