iores: iores.cpp
	g++ -Wall -O2 -o $@ $<

clean:
	rm -f iores
