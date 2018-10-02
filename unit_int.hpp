#pragma once

#include <string>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <cinttypes>
#include <stdarg.h>
#include "string_util.hpp"

/**
 * Convert size string with unit suffix to unsigned integer.
 */
inline uint64_t fromUnitIntString(const std::string &valStr)
{
    if (valStr.empty()) {
        throw std::runtime_error("fromUnitIntString: invalid argument.");
    }
    char *endp;
    uint64_t val = ::strtoll(valStr.c_str(), &endp, 10);
    int shift = 0;
    switch (*endp) {
    case 'e': case 'E': shift = 60; break;
    case 'p': case 'P': shift = 50; break;
    case 't': case 'T': shift = 40; break;
    case 'g': case 'G': shift = 30; break;
    case 'm': case 'M': shift = 20; break;
    case 'k': case 'K': shift = 10; break;
    case '\0': break;
    default:
        throw std::runtime_error("fromUnitIntString: invalid suffix charactor.");
    }
    if (((val << shift) >> shift) != val) {
        throw std::runtime_error("fromUnitIntString: overflow.");
    }
    return val << shift;
}

/**
 * Unit suffixes:
 *   k: 2^10
 *   m: 2^20
 *   g: 2^30
 *   t: 2^40
 *   p: 2^50
 *   e: 2^60
 */
inline std::string toUnitIntString(uint64_t val)
{
    uint64_t mask = (1ULL << 10) - 1;
    const char units[] = " kmgtpezy";

    size_t i = 0;
    while (i < sizeof(units)) {
        if ((val & ~mask) != val) { break; }
        i++;
        val >>= 10;
    }

    if (0 < i && i < sizeof(units)) {
        return formatString("%" PRIu64 "%c", val, units[i]);
    } else {
        return formatString("%" PRIu64 "", val);
    }
}
