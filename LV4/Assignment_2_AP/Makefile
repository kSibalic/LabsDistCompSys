CXX = g++
CXXFLAGS = -std=c++17 -pthread -Wall

all: leader follower client

leader: leader.cpp kv_store.h
	$(CXX) $(CXXFLAGS) leader.cpp -o leader

follower: follower.cpp kv_store.h
	$(CXX) $(CXXFLAGS) follower.cpp -o follower

client: client.cpp kv_store.h
	$(CXX) $(CXXFLAGS) client.cpp -o client

clean:
	rm -f leader follower client *.o *_seq.txt
