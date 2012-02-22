iores: iores.cpp util.hpp
	g++ -Wall -O2 -std=c++0x -pthread -o $@ $<

clean:
	rm -f iores
