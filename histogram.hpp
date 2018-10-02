#pragma once
#include <cinttypes>
#include <cstddef>
#include <vector>
#include <cstdio>


struct HistogramConfig
{
    // The unit of the time period is milliseconds.
    uint64_t min;
    uint64_t max;
    uint64_t interval;

    HistogramConfig() : min(0), max(0), interval(0) {}
    void set(uint64_t min0, uint64_t max0, uint64_t interval0) {
        min = min0;
        max = max0;
        interval = interval0;
        verify();
    }
    size_t get_bucket_size() const {
        verify();
        return (max - min) / interval;
    }
    void verify() const {
        if (min >= max) {
            throw std::runtime_error("HistogramConfig:min must be < max");
        }
        if (interval == 0) {
            throw std::runtime_error("HistogramConfig:interval must not be 0");
        }
        if ((max - min) % interval != 0) {
            throw std::runtime_error("HistogramConfig:(max - min) % interval must be 0");
        }
    }
    bool operator==(const HistogramConfig& rhs) const {
        return min == rhs.min && max == rhs.max && interval == rhs.interval;
    }
    bool operator!=(const HistogramConfig& rhs) const {
        return !operator==(rhs);
    }
};


class Histogram
{
    HistogramConfig cfg_;

    // counts.
    std::vector<size_t> buckets_;
    size_t min_count_;
    size_t max_count_;

public:
    Histogram() : cfg_(), buckets_(), min_count_(), max_count_() {
    }
    void reset(const HistogramConfig& cfg) {
        cfg_ = cfg;
        buckets_.clear();
        buckets_.resize(cfg.get_bucket_size());
        min_count_ = 0;
        max_count_ = 0;
    }
    void add(uint64_t response_ms) {
        if (response_ms < cfg_.min) {
            min_count_++;
        } else if (response_ms >= cfg_.max) {
            max_count_++;
        } else {
            size_t idx = (response_ms - cfg_.min) / cfg_.interval;
            assert(idx < buckets_.size());
            buckets_[idx]++;
        }
    }
    void merge(const Histogram& rhs) {
        assert(is_compatible(rhs));
        assert(buckets_.size() == rhs.buckets_.size());
        for (size_t i = 0; i < buckets_.size(); i++) {
            buckets_[i] += rhs.buckets_[i];
        }
        min_count_ += rhs.min_count_;
        max_count_ += rhs.max_count_;
    }
    void print() const {
        uint64_t key = cfg_.min;
        for (const uint64_t val : buckets_) {
            ::printf("%" PRIu64 " %zu\n", key, val);
            key += cfg_.interval;
        }
        assert(key == cfg_.max);
        ::printf("#under_min %zu over_max %zu\n", min_count_, max_count_);
    }
    static void joinAndPrint(const std::vector<Histogram>& hs) {
        if (hs.empty()) return;
        for (size_t i = 1; i < hs.size(); i++) {
            assert(hs[0].is_compatible(hs[i]));
        }
        uint64_t key = hs[0].cfg_.min;
        for (size_t bidx = 0; bidx < hs[0].buckets_.size(); bidx++) {
            ::printf("%" PRIu64 "", key);
            for (size_t i = 0; i < hs.size(); i++) {
                ::printf(" %zu", hs[i].buckets_[bidx]);
            }
            ::printf("\n");
            key += hs[0].cfg_.interval;
        }
        for (size_t i = 0; i < hs.size(); i++) {
            ::printf("# id %zu min %zu max %zu\n"
                     , i, hs[i].min_count_, hs[i].max_count_);
        }
    }

private:
    static size_t get_bucket_size(uint64_t min, uint64_t max, uint64_t interval) {
        assert(min < max);
        assert(interval > 0);
        assert((max - min) % interval == 0);
        return (max - min) / interval;
    }
    bool is_compatible(const Histogram& rhs) const {
        return cfg_ == rhs.cfg_;
    }
};
