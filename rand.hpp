#ifndef RAND_HPP
#define RAND_HPP

#include <random>

template<class T1, class T2>
class Rand
{
private:
    std::random_device rd_;
    std::mt19937 gen_;
    T2 dis_;

public:
    Rand(T1 min, T1 max)
        : rd_()
        , gen_(rd_())
        , dis_(min, max) {}

    T1 get() { return dis_(gen_); }
    T1 get(T1 max) { return dis_(gen_) % max; }
};

#endif //RAND_HPP
