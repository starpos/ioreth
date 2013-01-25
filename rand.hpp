#ifndef RAND_HPP
#define RAND_HPP

#include <random>
#include <cstdint>

/**
 * T1 example: uint32_t
 * T2 example: std::std::uniform_int_distribution<uint32_t>
 */
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

class XorShift128
{
private:
    uint32_t x_, y_, z_, w_;

public:
    explicit XorShift128(uint32_t seed)
        : x_(123456789)
        , y_(362436069)
        , z_(521288629)
        , w_(88675123) {

        x_ ^= seed;
        y_ ^= (seed << 8)  | (seed >> (32 - 8));
        z_ ^= (seed << 16) | (seed >> (32 - 16));
        w_ ^= (seed << 24) | (seed >> (32 - 24));
    }

    uint32_t get() {

        const uint32_t t = x_ ^ (x_ << 11);
        x_ = y_;
        y_ = z_;
        z_ = w_;
        w_ = (w_ ^ (w_ >> 19)) ^ (t ^ (t >> 8));
        return  w_;
    }

    uint32_t get(uint32_t max) {

        return get() % max;
    }
};

#endif //RAND_HPP
