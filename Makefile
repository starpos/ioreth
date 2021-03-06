.PHONY: all clean rebuild

CXX = g++
CFLAGS = -Wall -Wextra -std=c++11 -pthread
ifeq ($(DEBUG),1)
CFLAGS += -g -O0
else
CFLAGS += -O2
endif
ifeq ($(PROF),1)
CFLAGS += -pg
else
endif

ifeq ($(STATIC),1)
LDFLAGS = -static -laio -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
else
LDFLAGS = -laio -lpthread
endif

all: iores ioth

%: %.o
	$(CXX) $(CFLAGS) -o $@ $< $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CFLAGS) -c $<

iores.o: iores.cpp util.hpp ioreth.hpp rand.hpp
ioth.o: ioth.cpp util.hpp ioreth.hpp thread_pool.hpp

clean: cleanTest
	rm -f iores ioth *.o

rebuild:
	$(MAKE) clean
	$(MAKE) all

# for test.
sample_thread_pool.o: sample_thread_pool.cpp thread_pool.hpp
sample_thread_pool: sample_thread_pool.o
	$(CXX) $(CFLAGS) -o $@ $<

cleanTest:
	rm -f sample_thread_pool
