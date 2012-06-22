.PHONY: all clean

CXX = g++-4.7.1
CFLAGS = -Wall -g -std=c++11 -pthread -Wextra
ifeq ($(DEBUG),1)
CFLAGS += -g
else
CFLAGS += -O2
endif
ifeq ($(PROF),1)
CFLAGS += -pg
else
endif

LDFLAGS = -laio -lpthread

all: iores ioth

iores: iores.o
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $<
ioth: ioth.o
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $<

.cpp.o:
	$(CXX) $(CFLAGS) -c $<

iores.o: iores.cpp util.hpp ioreth.hpp rand.hpp
ioth.o: ioth.cpp util.hpp ioreth.hpp thread_pool.hpp

clean: cleanTest
	rm -f iores ioth *.o

# for test.
sample_thread_pool.o: sample_thread_pool.cpp thread_pool.hpp
sample_thread_pool: sample_thread_pool.o
	$(CXX) $(CFLAGS) -o $@ $<

cleanTest:
	rm -f sample_thread_pool
